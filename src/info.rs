// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::InfoCode;
use adbc_core::schemas::GET_INFO_SCHEMA;
use arrow_array::builder::{
    BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, UInt32Builder,
};
use arrow_array::{ArrayRef, RecordBatch, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, UnionFields};
use std::sync::Arc;

/// Builder for constructing the `get_info` response `RecordBatch`.
///
/// The ADBC `get_info` schema uses a Dense Union for the `info_value` column with 6 variants:
/// - 0: `string_value` (Utf8)
/// - 1: `bool_value` (Boolean)
/// - 2: `int64_value` (Int64)
/// - 3: `int32_bitmask` (Int32)
/// - 4: `string_list` (List<Utf8>)
/// - 5: `int32_to_int32_list_map` (Map<Int32, List<Int32>>)
pub struct GetInfoBuilder {
    name_builder: UInt32Builder,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,

    // Union variant builders (only the ones we actually use)
    string_builder: StringBuilder,
    string_offset: i32,
    bool_builder: BooleanBuilder,
    bool_offset: i32,
    int64_builder: Int64Builder,
    int64_offset: i32,
}

impl GetInfoBuilder {
    /// Creates a new `GetInfoBuilder`.
    pub fn new() -> Self {
        Self {
            name_builder: UInt32Builder::new(),
            type_ids: Vec::new(),
            offsets: Vec::new(),
            string_builder: StringBuilder::new(),
            string_offset: 0,
            bool_builder: BooleanBuilder::new(),
            bool_offset: 0,
            int64_builder: Int64Builder::new(),
            int64_offset: 0,
        }
    }

    /// Adds a string info value.
    pub fn add_string(&mut self, code: InfoCode, value: &str) {
        self.name_builder.append_value(u32::from(&code));
        self.string_builder.append_value(value);
        self.type_ids.push(0);
        self.offsets.push(self.string_offset);
        self.string_offset += 1;
    }

    /// Adds a boolean info value.
    pub fn add_bool(&mut self, code: InfoCode, value: bool) {
        self.name_builder.append_value(u32::from(&code));
        self.bool_builder.append_value(value);
        self.type_ids.push(1);
        self.offsets.push(self.bool_offset);
        self.bool_offset += 1;
    }

    /// Adds an int64 info value.
    pub fn add_int64(&mut self, code: InfoCode, value: i64) {
        self.name_builder.append_value(u32::from(&code));
        self.int64_builder.append_value(value);
        self.type_ids.push(2);
        self.offsets.push(self.int64_offset);
        self.int64_offset += 1;
    }

    /// Builds the final `RecordBatch` with the ADBC `get_info` schema.
    pub fn finish(mut self) -> Result<RecordBatch> {
        // Extract union fields from the schema
        let union_fields = match GET_INFO_SCHEMA
            .field_with_name("info_value")
            .map_err(|e| {
                Error::with_message_and_status(format!("Schema error: {e}"), Status::Internal)
            })?
            .data_type()
        {
            DataType::Union(fields, _) => fields.clone(),
            _ => {
                return Err(Error::with_message_and_status(
                    "Expected Union type for info_value".to_string(),
                    Status::Internal,
                ));
            }
        };

        // Build child arrays - must match the union field order
        // Arrays for types we use
        let string_array = Arc::new(self.string_builder.finish()) as ArrayRef;
        let bool_array = Arc::new(self.bool_builder.finish()) as ArrayRef;
        let int64_array = Arc::new(self.int64_builder.finish()) as ArrayRef;

        // Empty arrays for types we don't use (int32_bitmask, string_list)
        let int32_array = Arc::new(Int32Builder::new().finish()) as ArrayRef;
        let string_list_array =
            Arc::new(ListBuilder::new(StringBuilder::new()).finish()) as ArrayRef;

        // Build empty map array with correct schema
        let map_array = Self::build_empty_map_array(&union_fields)?;

        let type_id_buffer: ScalarBuffer<i8> = self.type_ids.into_iter().collect();
        let offsets_buffer: ScalarBuffer<i32> = self.offsets.into_iter().collect();

        let union_array = UnionArray::try_new(
            union_fields,
            type_id_buffer,
            Some(offsets_buffer),
            vec![
                string_array,
                bool_array,
                int64_array,
                int32_array,
                string_list_array,
                map_array,
            ],
        )
        .map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create UnionArray: {e}"),
                Status::Internal,
            )
        })?;

        RecordBatch::try_new(
            GET_INFO_SCHEMA.clone(),
            vec![Arc::new(self.name_builder.finish()), Arc::new(union_array)],
        )
        .map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create RecordBatch: {e}"),
                Status::Internal,
            )
        })
    }

    /// Builds an empty map array matching the schema's `int32_to_int32_list_map` field.
    fn build_empty_map_array(union_fields: &UnionFields) -> Result<ArrayRef> {
        use arrow_array::{Int32Array, ListArray, MapArray, StructArray};
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::Field;

        // Get the map field from union fields (index 5)
        let map_field = union_fields.iter().find(|(id, _)| *id == 5).map(|(_, f)| f);

        let map_field = map_field.ok_or_else(|| {
            Error::with_message_and_status(
                "Map field not found in union schema".to_string(),
                Status::Internal,
            )
        })?;

        // Extract the entries field from the map data type
        let entries_field = match map_field.data_type() {
            DataType::Map(entries_field, _) => entries_field.clone(),
            _ => {
                return Err(Error::with_message_and_status(
                    "Expected Map type".to_string(),
                    Status::Internal,
                ));
            }
        };

        // Build empty arrays for key (Int32) and value (List<Int32>)
        let empty_key_array: ArrayRef = Arc::new(Int32Array::new_null(0));

        // Create empty list array for values
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let empty_value_array: ArrayRef = Arc::new(ListArray::new(
            list_field,
            OffsetBuffer::new_empty(),
            Arc::new(Int32Array::new_null(0)),
            None,
        ));

        // Create empty struct array for entries
        let entries_struct = StructArray::new(
            match entries_field.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => {
                    return Err(Error::with_message_and_status(
                        "Expected Struct type for map entries".to_string(),
                        Status::Internal,
                    ));
                }
            },
            vec![empty_key_array, empty_value_array],
            None,
        );

        // Create empty map array with empty offsets
        let empty_map = MapArray::new(
            entries_field,
            OffsetBuffer::new_empty(),
            entries_struct,
            None,
            false,
        );

        Ok(Arc::new(empty_map))
    }
}

impl Default for GetInfoBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    #[test]
    fn test_get_info_builder_empty() {
        let builder = GetInfoBuilder::new();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
    }

    #[test]
    fn test_get_info_builder_string() {
        let mut builder = GetInfoBuilder::new();
        builder.add_string(InfoCode::VendorName, "Apache Druid");

        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());

        // Verify info_name column
        let name_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::UInt32Type>();
        assert_eq!(name_col.value(0), u32::from(&InfoCode::VendorName));

        // Verify info_value column (union)
        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(value_col.type_id(0), 0); // string type

        let string_value = value_col.value(0);
        let string_array = string_value.as_string::<i32>();
        assert_eq!(string_array.value(0), "Apache Druid");
    }

    #[test]
    fn test_get_info_builder_bool() {
        let mut builder = GetInfoBuilder::new();
        builder.add_bool(InfoCode::VendorSql, true);

        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1);

        let name_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::UInt32Type>();
        assert_eq!(name_col.value(0), u32::from(&InfoCode::VendorSql));

        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(value_col.type_id(0), 1); // bool type

        let bool_value = value_col.value(0);
        let bool_array = bool_value.as_boolean();
        assert!(bool_array.value(0));
    }

    #[test]
    fn test_get_info_builder_int64() {
        let mut builder = GetInfoBuilder::new();
        builder.add_int64(InfoCode::DriverAdbcVersion, 1_001_000);

        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1);

        let name_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::UInt32Type>();
        assert_eq!(name_col.value(0), u32::from(&InfoCode::DriverAdbcVersion));

        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(value_col.type_id(0), 2); // int64 type

        let int64_value = value_col.value(0);
        let int64_array = int64_value.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(int64_array.value(0), 1_001_000);
    }

    #[test]
    fn test_get_info_builder_multiple_values() {
        let mut builder = GetInfoBuilder::new();
        builder.add_string(InfoCode::VendorName, "Apache Druid");
        builder.add_bool(InfoCode::VendorSql, true);
        builder.add_bool(InfoCode::VendorSubstrait, false);
        builder.add_string(InfoCode::DriverName, "ADBC Druid Driver");
        builder.add_int64(InfoCode::DriverAdbcVersion, 1_001_000);

        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());

        // Verify type IDs
        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(value_col.type_id(0), 0); // string
        assert_eq!(value_col.type_id(1), 1); // bool
        assert_eq!(value_col.type_id(2), 1); // bool
        assert_eq!(value_col.type_id(3), 0); // string
        assert_eq!(value_col.type_id(4), 2); // int64
    }

    #[test]
    fn test_schema_matches_adbc_spec() {
        let builder = GetInfoBuilder::new();
        let batch = builder.finish().unwrap();

        // Verify schema exactly matches ADBC spec
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());

        // Verify field names
        assert_eq!(batch.schema().field(0).name(), "info_name");
        assert_eq!(batch.schema().field(1).name(), "info_value");

        // Verify info_name is UInt32
        assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt32);

        // Verify info_value is a Dense Union
        match batch.schema().field(1).data_type() {
            DataType::Union(_, mode) => {
                assert_eq!(*mode, arrow_schema::UnionMode::Dense);
            }
            _ => panic!("Expected Union type"),
        }
    }
}
