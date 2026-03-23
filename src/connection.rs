use crate::batch_reader::SingleBatchReader;
use crate::statement::DruidStatement;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use std::collections::HashSet;

pub struct DruidConnection {}

impl Connection for DruidConnection {
    type StatementType = DruidStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(DruidStatement {})
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "cancel not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_info not implemented".to_string(),
            Status::NotImplemented,
        ))
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
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_table_types not implemented".to_string(),
            Status::NotImplemented,
        ))
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
