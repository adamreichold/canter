pub mod error;
pub mod query;
pub mod reader;
pub mod tokenizer;
pub mod writer;

use std::ops::{Deref, DerefMut};

use hashbrown::hash_map::{EntryRef, HashMap};
use rusqlite::{Connection, OptionalExtension, functions::FunctionFlags, params};

use crate::{
    error::Error,
    tokenizer::{
        ErasedTokenizer, LimitLength, SplitNonAlphanumeric, StubTokenizer, ToLowerCase, Tokenizer,
    },
};

#[non_exhaustive]
pub struct Config {
    pub bm25_k1: f64,
    pub bm25_b: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bm25_k1: 2.0,
            bm25_b: 0.75,
        }
    }
}

pub struct Index {
    conn: Connection,
    tokenizers: Tokenizers,
    fields: Fields,
}

impl Deref for Index {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for Index {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl Index {
    pub fn open(mut conn: Connection, config: Config) -> Result<Self, Error> {
        let bm25_k1 = config.bm25_k1;
        let bm25_b = config.bm25_b;

        conn.create_scalar_function(
            "canter_bm25",
            5,
            FunctionFlags::SQLITE_DETERMINISTIC,
            move |ctx| {
                let documents = ctx.get::<usize>(0)? as f64;
                let avg_documents_count = ctx.get::<f64>(1)?;
                let terms_count = ctx.get::<usize>(2)? as f64;
                let postings_count = ctx.get::<usize>(3)? as f64;
                let documents_count = ctx.get::<usize>(4)? as f64;

                let idf = ((documents - terms_count + 0.5) / (terms_count + 0.5) + 1.0).ln();

                Ok(idf * (postings_count * (bm25_k1 + 1.0))
                    / (postings_count
                        + bm25_k1
                            * (1.0 - bm25_b + bm25_b * documents_count / avg_documents_count)))
            },
        )?;

        let txn = conn.transaction()?;

        txn.execute_batch(
            r#"CREATE TABLE IF NOT EXISTS canter_fields (
                   id INTEGER PRIMARY KEY,
                   name TEXT NOT NULL UNIQUE,
                   tokenizer TEXT NOT NULL
               );

               CREATE TABLE IF NOT EXISTS canter_terms (
                   id INTEGER PRIMARY KEY,
                   field_id INTEGER NOT NULL,
                   value TEXT NOT NULL,
                   count INTEGER NOT NULL,
                   UNIQUE (field_id, value)
               );

               CREATE TABLE IF NOT EXISTS canter_postings (
                   term_id INTEGER NOT NULL,
                   document_id INTEGER NOT NULL,
                   position INTEGER NOT NULL,
                   PRIMARY KEY (term_id, document_id, position)
               )
               WITHOUT ROWID;

               CREATE TABLE IF NOT EXISTS canter_documents (
                   field_id INTEGER NOT NULL,
                   document_id INTEGER NOT NULL,
                   count INTEGER NOT NULL,
                   PRIMARY KEY (field_id, document_id)
               )
               WITHOUT ROWID;"#,
        )?;

        txn.commit()?;

        let tokenizers = [
            ("stub".to_owned(), StubTokenizer.into()),
            (
                "default".to_owned(),
                SplitNonAlphanumeric
                    .chain(LimitLength::default())
                    .chain(ToLowerCase::default())
                    .into(),
            ),
        ]
        .into_iter()
        .collect();

        Ok(Self {
            conn,
            tokenizers,
            fields: HashMap::new(),
        })
    }

    pub fn add_field(&mut self, name: &str, tokenizer: &str) -> Result<(), Error> {
        let txn = self.conn.transaction()?;

        {
            let existing_tokenizer = txn
                .query_row(
                    "SELECT tokenizer FROM canter_fields WHERE name = ?",
                    params![name],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;

            match existing_tokenizer {
                Some(existing_tokenizer) if existing_tokenizer == tokenizer => return Ok(()),
                Some(existing_tokenizer) => {
                    return Err(Error::FieldConflict {
                        name: name.to_owned(),
                        tokenizer: tokenizer.to_owned(),
                        existing_tokenizer,
                    });
                }
                None => (),
            }
        }

        {
            let mut stmt =
                txn.prepare("INSERT INTO canter_fields (name, tokenizer) VALUES (?, ?)")?;

            stmt.execute(params![name, tokenizer])?;
        }

        txn.commit()?;

        Ok(())
    }
}

type Tokenizers = HashMap<String, Box<dyn ErasedTokenizer>>;

struct Field {
    id: i64,
    tokenizer: String,
    documents: usize,
    avg_documents_count: f64,
}

type Fields = HashMap<String, Field>;

fn read_field<'fields>(
    conn: &Connection,
    fields: &'fields mut Fields,
    name: &str,
) -> Result<&'fields Field, Error> {
    match fields.entry_ref(name) {
        EntryRef::Occupied(entry) => Ok(entry.into_mut()),
        EntryRef::Vacant(entry) => {
            let mut stmt = conn.prepare(
                r#"SELECT
                       canter_fields.id, canter_fields.tokenizer,
                       COUNT(canter_documents.document_id), AVG(canter_documents.count)
                   FROM canter_fields LEFT JOIN canter_documents
                   ON canter_fields.id = canter_documents.field_id
                   WHERE canter_fields.name = ? GROUP BY canter_fields.id"#,
            )?;

            let field = stmt
                .query_row(params![name], |row| {
                    let id = row.get(0)?;
                    let tokenizer = row.get(1)?;

                    let documents = row.get::<_, Option<usize>>(2)?.unwrap_or(0);
                    let avg_documents_count = row.get::<_, Option<f64>>(3)?.unwrap_or(0.0);

                    Ok(Field {
                        id,
                        tokenizer,
                        documents,
                        avg_documents_count,
                    })
                })
                .optional()?;

            match field {
                Some(field) => Ok(entry.insert(field)),
                None => Err(Error::NoSuchField(name.to_owned())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::Index;

    #[test]
    fn it_works() {
        let conn = Connection::open_in_memory().unwrap();

        let mut index = Index::open(conn, Default::default()).unwrap();

        index.add_field("field", "default").unwrap();

        {
            let mut reader = index.read().unwrap();

            let query = reader.parse("field:foo").unwrap();
            let results = reader.search(&*query).unwrap();
            assert_eq!(results, []);
        }

        {
            let mut writer = index.rewrite().unwrap();

            writer.add_text(1, "field", "FOO bar").unwrap();
            writer.add_text(1, "field", "BAZ").unwrap();

            writer.add_text(2, "field", "foo").unwrap();
            writer.add_text(3, "field", "BAR").unwrap();
            writer.add_text(4, "field", "baz").unwrap();

            writer.commit().unwrap();
        }

        let mut reader = index.read().unwrap();

        let query = reader.parse("field:foo field:bar").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(
            results,
            [
                (1, 1.8483924814931874),
                (2, 0.8317766166719343),
                (3, 0.8317766166719343)
            ]
        );

        let query = reader.parse("+field:foo +field:bar +field:baz").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, [(1, 4.1588830833596715)]);

        let query = reader.parse("+field:foo field:bar").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, [(1, 1.8483924814931874), (2, 0.8317766166719343)]);

        let query = reader.parse("+field:bar -field:foo -field:baz").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, [(3, 0.8317766166719343)]);

        let query = reader.parse("-field:foo").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, [(3, 1.0), (4, 1.0)]);

        let query = reader.parse("field:\"bar baz\"").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, [(1, 1.8483924814931874)]);

        let query = reader.parse("field:\"foo baz\"").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, []);

        let query = reader.parse("field:foo -field:\"bar baz\"").unwrap();
        let results = reader.search(&*query).unwrap();
        assert_eq!(results, [(2, 0.8317766166719343)]);
    }
}
