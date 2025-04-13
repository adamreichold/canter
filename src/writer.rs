use std::ops::Deref;

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::{Fields, Index, Tokenizers, error::Error, read_field};

impl Index {
    pub fn rewrite(&mut self) -> Result<Writer<'_>, Error> {
        let txn = self.conn.transaction()?;

        txn.execute_batch(
            r#"DELETE FROM canter_terms;
               DELETE FROM canter_postings;
               DELETE FROM canter_documents;"#,
        )?;

        let cnt = txn.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_sequence'",
            (),
            |row| row.get::<_, usize>(0),
        )?;

        if cnt != 0 {
            txn.execute(
                "DELETE FROM sqlite_sequence WHERE name IN ('canter_terms', 'canter_postings', 'canter_documents')",
                (),
            )?;
        }

        Ok(Writer {
            txn,
            tokenizers: &mut self.tokenizers,
            fields: &mut self.fields,
        })
    }
}

pub struct Writer<'index> {
    txn: Transaction<'index>,
    tokenizers: &'index mut Tokenizers,
    fields: &'index mut Fields,
}

impl Deref for Writer<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl Writer<'_> {
    pub fn add_text(
        &mut self,
        document_id: i64,
        field_name: &str,
        text: &str,
    ) -> Result<(), Error> {
        let field = read_field(&self.txn, self.fields, field_name)?;

        let tokenizer = self
            .tokenizers
            .get_mut(&field.tokenizer)
            .ok_or_else(|| Error::NoSuchTokenizer(field.tokenizer.clone()))?;

        let mut position = reset_position(&self.txn, field.id, document_id)?;

        tokenizer.erased_tokenize(text, &mut |token| {
            position += 1;

            let term_id = add_term(&self.txn, field.id, token)?;
            add_posting(&self.txn, term_id, document_id, position)?;

            Ok(())
        })?;

        add_document(&self.txn, field.id, document_id, position)?;

        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        self.txn.execute_batch(
            r#"ANALYZE canter_fields;
               ANALYZE canter_terms;
               ANALYZE canter_postings;
               ANALYZE canter_documents;"#,
        )?;

        self.txn.commit()?;

        self.fields.clear();

        Ok(())
    }
}

fn add_term(conn: &Connection, field_id: i64, value: &str) -> Result<i64, Error> {
    let mut stmt =
        conn.prepare_cached("SELECT id FROM canter_terms WHERE field_id = ? AND value = ?")?;

    let term_id = stmt
        .query_row(params![field_id, value], |row| row.get::<_, i64>(0))
        .optional()?;

    if let Some(term_id) = term_id {
        let mut stmt =
            conn.prepare_cached("UPDATE canter_terms SET count = count + 1 WHERE id = ?")?;

        stmt.execute(params![term_id])?;

        Ok(term_id)
    } else {
        let mut stmt = conn
            .prepare_cached("INSERT INTO canter_terms (field_id, value, count) VALUES (?, ?, 1)")?;

        stmt.execute(params![field_id, value])?;

        Ok(conn.last_insert_rowid())
    }
}

fn add_posting(
    conn: &Connection,
    term_id: i64,
    document_id: i64,
    position: usize,
) -> Result<(), Error> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO canter_postings (term_id, document_id, position) VALUES (?, ?, ?)",
    )?;

    stmt.execute(params![term_id, document_id, position])?;

    Ok(())
}

fn add_document(
    conn: &Connection,
    field_id: i64,
    document_id: i64,
    position: usize,
) -> Result<(), Error> {
    let mut stmt = conn.prepare_cached("INSERT INTO canter_documents (field_id, document_id, count) VALUES (?1, ?2, ?3) ON CONFLICT DO UPDATE SET count = ?3")?;

    stmt.execute(params![field_id, document_id, position])?;

    Ok(())
}

fn reset_position(conn: &Connection, field_id: i64, document_id: i64) -> Result<usize, Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT count FROM canter_documents WHERE field_id = ? AND document_id = ?",
    )?;

    let position = stmt
        .query_row(params![field_id, document_id], |row| row.get::<_, usize>(0))
        .optional()?;

    Ok(position.unwrap_or(0))
}
