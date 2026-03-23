use crate::batch_reader::SingleBatchReader;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

pub struct DruidStatement {}

impl Statement for DruidStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(Error::with_message_and_status(
            "bind not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(Error::with_message_and_status(
            "bind_stream not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "execute not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(Error::with_message_and_status(
            "execute_update not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "execute_schema not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(Error::with_message_and_status(
            "execute_partitions not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "get_parameter_schema not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "prepare not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        Err(Error::with_message_and_status(
            "set_sql_query not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(Error::with_message_and_status(
            "set_substrait_plan not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "cancel not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}

impl Optionable for DruidStatement {
    type Option = OptionStatement;

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
