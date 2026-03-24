use crate::connection::DruidConnection;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};

#[derive(Debug, Default)]
pub struct DruidDatabase {
    uri: Option<String>,
}

impl DruidDatabase {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Database for DruidDatabase {
    type ConnectionType = DruidConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(vec![])
    }

    fn new_connection_with_opts(
        &self,
        _opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let uri = self.uri.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "Database URI not set. Use set_option with OptionDatabase::Uri".to_string(),
                Status::InvalidState,
            )
        })?;
        DruidConnection::new(uri)
    }
}

impl Optionable for DruidDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match (key, value) {
            (OptionDatabase::Uri, OptionValue::String(uri)) => {
                self.uri = Some(uri);
                Ok(())
            }
            (OptionDatabase::Uri, _) => Err(Error::with_message_and_status(
                "URI must be a string".to_string(),
                Status::InvalidArguments,
            )),
            (key, _) => Err(Error::with_message_and_status(
                format!("Unsupported option: {key:?}"),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => self.uri.clone().ok_or_else(|| {
                Error::with_message_and_status("URI not set".to_string(), Status::NotFound)
            }),
            _ => Err(Error::with_message_and_status(
                format!("Unsupported option: {key:?}"),
                Status::NotImplemented,
            )),
        }
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

    #[test]
    fn test_set_uri_option() {
        let mut db = DruidDatabase::new();
        let result = db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(db.uri, Some("http://localhost:8888".to_string()));
    }

    #[test]
    fn test_get_uri_option() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        )
        .unwrap();
        let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
        assert_eq!(uri, "http://localhost:8888");
    }

    #[test]
    fn test_get_uri_option_not_set() {
        let db = DruidDatabase::new();
        let result = db.get_option_string(OptionDatabase::Uri);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotFound);
    }

    #[test]
    fn test_new_connection_without_uri_fails() {
        let db = DruidDatabase::new();
        let result = db.new_connection();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidState);
    }

    #[test]
    fn test_new_connection_with_uri() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        )
        .unwrap();
        let result = db.new_connection();
        assert!(result.is_ok());
    }
}
