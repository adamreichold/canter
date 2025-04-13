use std::ops::Deref;

use rusqlite::{Connection, Transaction};
use smallvec::SmallVec;

use crate::{
    Fields, Index, Tokenizers,
    error::Error,
    query::{CombinedQuery, Occur, PhraseQuery, Query, TermQuery},
    read_field,
    tokenizer::ErasedTokenizer,
};

impl Index {
    pub fn read(&mut self) -> Result<Reader<'_>, Error> {
        let txn = self.conn.transaction()?;

        Ok(Reader {
            txn,
            tokenizers: &mut self.tokenizers,
            fields: &mut self.fields,
        })
    }
}

pub struct Reader<'index> {
    txn: Transaction<'index>,
    tokenizers: &'index mut Tokenizers,
    fields: &'index mut Fields,
}

impl Deref for Reader<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl Reader<'_> {
    pub fn parse(&mut self, text: &str) -> Result<Box<dyn Query>, Error> {
        let (query, text) = self.parse_clauses(text.trim_start())?;
        assert!(text.is_empty());

        Ok(Box::new(query))
    }

    fn parse_clauses<'text>(
        &mut self,
        mut text: &'text str,
    ) -> Result<(CombinedQuery, &'text str), Error> {
        let mut clauses = Vec::new();

        while !text.is_empty() {
            let (occur, clause, rest) = self.parse_clause(text)?;
            clauses.push((occur, clause));
            text = rest;
        }

        Ok((CombinedQuery::new(clauses), text))
    }

    fn parse_clause<'text>(
        &mut self,
        text: &'text str,
    ) -> Result<(Occur, Box<dyn Query>, &'text str), Error> {
        let (occur, text) = parse_occur(text);
        let (field_name, text) = parse_field_name(text)?;

        let field = read_field(&self.txn, self.fields, field_name)?;

        let tokenizer = self
            .tokenizers
            .get_mut(&field.tokenizer)
            .ok_or_else(|| Error::NoSuchTokenizer(field.tokenizer.clone()))?;

        let (mut values, rest) = parse_values(tokenizer, text)?;

        let query = match values.len() {
            0 => return Err(Error::InvalidValue(text.to_owned())),
            1 => TermQuery::new(field, values.pop().unwrap()).into(),
            _ => PhraseQuery::new(field, values.into_vec()).into(),
        };

        Ok((occur, query, rest.trim_start()))
    }

    pub fn search(&self, query: &dyn Query) -> Result<Vec<(i64, f64)>, Error> {
        let mut sql = "SELECT document_id, score FROM (\n".to_owned();
        let mut params = Vec::new();

        query.to_sql(true, &mut sql, &mut params);

        sql.push_str("\n) ORDER BY score DESC");

        let mut results = Vec::new();

        let mut stmt = self.txn.prepare(&sql)?;
        let mut rows = stmt.query(&*params)?;

        while let Some(row) = rows.next()? {
            let document_id = row.get::<_, i64>(0)?;
            let score = row.get::<_, f64>(1)?;

            results.push((document_id, score));
        }

        Ok(results)
    }
}

fn parse_occur(text: &str) -> (Occur, &str) {
    if let Some(text) = text.strip_prefix("+") {
        (Occur::Must, text)
    } else if let Some(text) = text.strip_prefix("-") {
        (Occur::MustNot, text)
    } else {
        (Occur::Should, text)
    }
}

fn parse_field_name(text: &str) -> Result<(&str, &str), Error> {
    let pos = text
        .find(':')
        .ok_or_else(|| Error::MissingFieldName(text.to_owned()))?;

    let field_name = &text[..pos];
    let text = &text[pos + 1..];

    Ok((field_name, text))
}

fn parse_values<'text>(
    tokenizer: &mut Box<dyn ErasedTokenizer>,
    text: &'text str,
) -> Result<(SmallVec<[String; 1]>, &'text str), Error> {
    let (value, text) = match text.strip_prefix("\"") {
        Some(text) => {
            let pos = text
                .find('"')
                .ok_or_else(|| Error::UnclosedQuote(text.to_owned()))?;

            (&text[..pos], &text[pos + 1..])
        }
        None => {
            let pos = text.find(char::is_whitespace).unwrap_or(text.len());

            text.split_at(pos)
        }
    };

    let mut values = SmallVec::new();

    tokenizer.erased_tokenize(value, &mut |token| {
        values.push(token.to_owned());

        Ok(())
    })?;

    Ok((values, text))
}
