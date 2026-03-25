use crate::batch_reader::SingleBatchReader;
use crate::client::DruidClient;
use crate::info::GetInfoBuilder;
use crate::statement::DruidStatement;
use adbc_core::constants::ADBC_VERSION_1_1_0;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::schemas::GET_TABLE_TYPES_SCHEMA;
use adbc_core::{Connection, Optionable};
use arrow_array::builder::StringBuilder;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use std::collections::HashSet;
use std::sync::Arc;

/// The set of info codes supported by this driver.
const SUPPORTED_INFO_CODES: &[InfoCode] = &[
    InfoCode::VendorName,
    InfoCode::VendorVersion,
    InfoCode::VendorSql,
    InfoCode::VendorSubstrait,
    InfoCode::DriverName,
    InfoCode::DriverVersion,
    InfoCode::DriverArrowVersion,
    InfoCode::DriverAdbcVersion,
];

/// Arrow library version used by this driver.
const ARROW_VERSION: &str = "57";

/// Table types supported by Druid.
const SUPPORTED_TABLE_TYPES: &[&str] = &["TABLE", "SYSTEM TABLE"];

#[derive(Debug)]
pub struct DruidConnection {
    client: Arc<DruidClient>,
}

impl DruidConnection {
    /// Creates a new `DruidConnection` with the given URI.
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn new(uri: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: Arc::new(DruidClient::new(uri)?),
        })
    }
}

impl Connection for DruidConnection {
    type StatementType = DruidStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(DruidStatement::new(Arc::clone(&self.client)))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "cancel not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        let mut builder = GetInfoBuilder::new();

        // Determine which codes to return
        let codes_to_return: Vec<InfoCode> = match codes {
            Some(requested) => {
                // Filter to only supported codes, preserving request order isn't required
                let supported: HashSet<InfoCode> = SUPPORTED_INFO_CODES.iter().copied().collect();
                requested.intersection(&supported).copied().collect()
            }
            None => SUPPORTED_INFO_CODES.to_vec(),
        };

        for code in codes_to_return {
            match code {
                InfoCode::VendorName => builder.add_string(code, "Apache Druid"),
                InfoCode::VendorVersion => {
                    let version = self.client.get_server_version();
                    builder.add_string(code, &version);
                }
                InfoCode::VendorSql => builder.add_bool(code, true),
                InfoCode::VendorSubstrait => builder.add_bool(code, false),
                InfoCode::DriverName => builder.add_string(code, "ADBC Druid Driver"),
                InfoCode::DriverVersion => builder.add_string(code, env!("CARGO_PKG_VERSION")),
                InfoCode::DriverArrowVersion => builder.add_string(code, ARROW_VERSION),
                InfoCode::DriverAdbcVersion => {
                    builder.add_int64(code, i64::from(ADBC_VERSION_1_1_0));
                }
                _ => {} // Silently ignore unsupported codes (shouldn't happen due to filter above)
            }
        }

        let batch = builder.finish()?;
        Ok(SingleBatchReader::new(batch))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_objects not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "get_table_schema not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        let mut builder = StringBuilder::new();
        for table_type in SUPPORTED_TABLE_TYPES {
            builder.append_value(table_type);
        }

        let batch = RecordBatch::try_new(
            GET_TABLE_TYPES_SCHEMA.clone(),
            vec![Arc::new(builder.finish())],
        )
        .map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create RecordBatch: {e}"),
                Status::Internal,
            )
        })?;

        Ok(SingleBatchReader::new(batch))
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_statistic_names not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_statistics not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "commit not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "rollback not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "read_partition not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}

impl Optionable for DruidConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        Err(Error::with_message_and_status(
            "set_option not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(
            "get_option_string not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "get_option_bytes not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "get_option_int not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "get_option_double not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    #[test]
    fn test_get_table_types_returns_correct_schema() {
        let conn = DruidConnection::new("http://localhost:8888").unwrap();
        let reader = conn.get_table_types().unwrap();

        assert_eq!(reader.schema(), GET_TABLE_TYPES_SCHEMA.clone());
    }

    #[test]
    fn test_get_table_types_returns_table_and_system_table() {
        let conn = DruidConnection::new("http://localhost:8888").unwrap();
        let mut reader = conn.get_table_types().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);

        let col = batch.column(0).as_string::<i32>();
        let types: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();

        assert!(types.contains(&"TABLE"));
        assert!(types.contains(&"SYSTEM TABLE"));
    }
}
