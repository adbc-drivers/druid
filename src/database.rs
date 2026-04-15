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

use crate::connection::DruidConnection;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};

fn require_string(value: OptionValue, name: &str) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(Error::with_message_and_status(
            format!("{name} must be a string"),
            Status::InvalidArguments,
        )),
    }
}

fn get_or_not_found(opt: Option<&String>, name: &str) -> Result<String> {
    opt.cloned()
        .ok_or_else(|| Error::with_message_and_status(format!("{name} not set"), Status::NotFound))
}

#[derive(Debug, Default)]
pub struct DruidDatabase {
    uri: Option<String>,
    username: Option<String>,
    password: Option<String>,
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
        DruidConnection::new(uri, self.username.clone(), self.password.clone())
    }
}

impl Optionable for DruidDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionDatabase::Uri => self.uri = Some(require_string(value, "URI")?),
            OptionDatabase::Username => self.username = Some(require_string(value, "Username")?),
            OptionDatabase::Password => self.password = Some(require_string(value, "Password")?),
            _ => {
                return Err(Error::with_message_and_status(
                    format!("Unsupported option: {key:?}"),
                    Status::NotImplemented,
                ));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => get_or_not_found(self.uri.as_ref(), "URI"),
            OptionDatabase::Username => get_or_not_found(self.username.as_ref(), "Username"),
            OptionDatabase::Password => get_or_not_found(self.password.as_ref(), "Password"),
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

    #[test]
    fn test_set_username_option() {
        let mut db = DruidDatabase::new();
        let result = db.set_option(
            OptionDatabase::Username,
            OptionValue::String("admin".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Username).unwrap(),
            "admin"
        );
    }

    #[test]
    fn test_set_password_option() {
        let mut db = DruidDatabase::new();
        let result = db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Password).unwrap(),
            "secret"
        );
    }

    #[test]
    fn test_get_username_option() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Username,
            OptionValue::String("admin".to_string()),
        )
        .unwrap();
        let username = db.get_option_string(OptionDatabase::Username).unwrap();
        assert_eq!(username, "admin");
    }

    #[test]
    fn test_get_password_option() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".to_string()),
        )
        .unwrap();
        let password = db.get_option_string(OptionDatabase::Password).unwrap();
        assert_eq!(password, "secret");
    }

    #[test]
    fn test_get_username_not_set() {
        let db = DruidDatabase::new();
        let result = db.get_option_string(OptionDatabase::Username);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_password_not_set() {
        let db = DruidDatabase::new();
        let result = db.get_option_string(OptionDatabase::Password);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_username_must_be_string() {
        let mut db = DruidDatabase::new();
        let result = db.set_option(OptionDatabase::Username, OptionValue::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_password_must_be_string() {
        let mut db = DruidDatabase::new();
        let result = db.set_option(OptionDatabase::Password, OptionValue::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_new_connection_with_credentials() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Username,
            OptionValue::String("admin".to_string()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".to_string()),
        )
        .unwrap();
        let result = db.new_connection();
        assert!(result.is_ok());
    }

    #[test]
    fn test_new_connection_with_username_only_fails() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Username,
            OptionValue::String("admin".to_string()),
        )
        .unwrap();
        let result = db.new_connection();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }

    #[test]
    fn test_new_connection_with_password_only_fails() {
        let mut db = DruidDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("http://localhost:8888".to_string()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".to_string()),
        )
        .unwrap();
        let result = db.new_connection();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }
}
