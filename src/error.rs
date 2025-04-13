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
    MissingFieldName(String),
    UnclosedQuote(String),
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
            Self::MissingFieldName(text) => write!(fmt, "Missing field name: {text}"),
            Self::UnclosedQuote(text) => write!(fmt, "Unclosed quote: {text}"),
            Self::InvalidValue(text) => write!(fmt, "Invalid value: {text}"),
        }
    }
}

impl From<SqliteError> for Error {
    fn from(err: SqliteError) -> Self {
        Self::Sqlite(err)
    }
}
