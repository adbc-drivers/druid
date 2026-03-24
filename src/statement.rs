use crate::batch_reader::SingleBatchReader;
use crate::client::DruidClient;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use std::sync::Arc;

#[derive(Debug)]
pub struct DruidStatement {
    client: Arc<DruidClient>,
    sql_query: Option<String>,
}

impl DruidStatement {
    #[must_use]
    pub fn new(client: Arc<DruidClient>) -> Self {
        Self {
            client,
            sql_query: None,
        }
    }

    fn query(&self) -> Result<&str> {
        self.sql_query.as_deref().ok_or_else(|| {
            Error::with_message_and_status(
                "No SQL query set. Call set_sql_query first.".to_string(),
                Status::InvalidState,
            )
        })
    }
}

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
        let batch = self.client.execute_query(self.query()?)?;
        Ok(SingleBatchReader::new(batch))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        // Execute the query and discard the result batch. Druid's SQL API
        // doesn't return affected row counts for DML/DDL statements.
        let _result = self.client.execute_query(self.query()?)?;
        Ok(None)
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

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_client() -> Arc<DruidClient> {
        Arc::new(DruidClient::new("http://localhost:8888").unwrap())
    }

    #[test]
    fn test_set_sql_query() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.set_sql_query("SELECT 1");
        assert!(result.is_ok());
        assert_eq!(stmt.sql_query, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_set_sql_query_overwrites_previous() {
        let mut stmt = DruidStatement::new(create_test_client());
        stmt.set_sql_query("SELECT 1").unwrap();
        stmt.set_sql_query("SELECT 2").unwrap();
        assert_eq!(stmt.sql_query, Some("SELECT 2".to_string()));
    }

    #[test]
    fn test_execute_update_without_query_returns_error() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidState);
    }
}
