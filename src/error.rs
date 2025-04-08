use std::error::Error as StdError;
use std::fmt;

use rusqlite::Error as SqliteError;

#[derive(Debug)]
pub enum Error {
    Sqlite(SqliteError),
    FieldConflict {
        name: String,
        tokenizer: String,
        existing_tokenizer: String,
    },
    NoSuchField(String),
    NoSuchTokenizer(String),
    DisconnectedWriter,
    DisconnectedSource,
    MissingFieldName(String),
    InvalidValue(String),
}

impl StdError for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(err) => write!(fmt, "SQLite error: {err}"),
            Self::FieldConflict {
                name,
                tokenizer,
                existing_tokenizer,
            } => write!(
                fmt,
                "Field `{name}` already defined, but using tokenizer `{existing_tokenizer}` instead of `{tokenizer}"
            ),
            Self::NoSuchField(name) => write!(fmt, "No such field: {name}"),
            Self::NoSuchTokenizer(name) => write!(fmt, "No such tokenizer: {name}"),
            Self::DisconnectedWriter => write!(fmt, "Disconnected writer"),
            Self::DisconnectedSource => write!(fmt, "Disconnected source"),
            Self::MissingFieldName(text) => write!(fmt, "Missing field name: {text}"),
            Self::InvalidValue(value) => write!(fmt, "Invalid value: {value}"),
        }
    }
}

impl From<SqliteError> for Error {
    fn from(err: SqliteError) -> Self {
        Self::Sqlite(err)
    }
}
