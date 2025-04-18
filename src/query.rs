use std::fmt::Write;

use rusqlite::ToSql;

use crate::Field;

pub trait Query {
    fn to_sql<'query>(
        &'query self,
        score: bool,
        sql: &mut String,
        params: &mut Vec<&'query dyn ToSql>,
    );
}

impl<Q> From<Q> for Box<dyn Query>
where
    Q: Query + 'static,
{
    fn from(query: Q) -> Self {
        Box::new(query)
    }
}

pub struct AllQuery;

impl Query for AllQuery {
    fn to_sql<'query>(
        &'query self,
        score: bool,
        sql: &mut String,
        _params: &mut Vec<&'query dyn ToSql>,
    ) {
        sql.push_str(if score {
            "SELECT DISTINCT document_id, 1 AS score, 1 as terms FROM canter_postings"
        } else {
            "SELECT DISTINCT document_id FROM canter_postings"
        });
    }
}

pub struct TermQuery {
    field_id: i64,
    documents: usize,
    avg_documents_count: f64,
    boost: f64,
    value: String,
}

impl TermQuery {
    pub(crate) fn new(field: &Field, boost: f64, value: String) -> Self {
        Self {
            field_id: field.id,
            documents: field.documents,
            avg_documents_count: field.avg_documents_count,
            boost,
            value,
        }
    }
}

impl Query for TermQuery {
    fn to_sql<'query>(
        &'query self,
        score: bool,
        sql: &mut String,
        params: &mut Vec<&'query dyn ToSql>,
    ) {
        if score {
            write!(
                sql,
                r#"SELECT canter_postings.document_id AS document_id,
                   {} * canter_bm25({}, {},
                       canter_terms.count,
                       COUNT(canter_postings.position),
                       canter_documents.count) AS score,
                   1 as terms"#,
                self.boost, self.documents, self.avg_documents_count
            )
            .unwrap();
        } else {
            sql.push_str("SELECT canter_postings.document_id AS document_id");
        }

        sql.push_str(
            " FROM canter_terms\nJOIN canter_postings ON canter_terms.id = canter_postings.term_id",
        );

        if score {
            sql.push_str("\nJOIN canter_documents ON canter_terms.field_id = canter_documents.field_id AND canter_postings.document_id = canter_documents.document_id");
        }

        write!(
            sql,
            "\nWHERE canter_terms.field_id = {} AND canter_terms.value = ? GROUP BY canter_postings.term_id, canter_postings.document_id",
            self.field_id
        )
        .unwrap();

        params.push(&self.value);
    }
}

pub struct PhraseQuery {
    field_id: i64,
    documents: usize,
    avg_documents_count: f64,
    boost: f64,
    values: Vec<String>,
}

impl PhraseQuery {
    pub(crate) fn new(field: &Field, boost: f64, values: Vec<String>) -> Self {
        Self {
            field_id: field.id,
            documents: field.documents,
            avg_documents_count: field.avg_documents_count,
            boost,
            values,
        }
    }
}

impl Query for PhraseQuery {
    fn to_sql<'query>(
        &'query self,
        score: bool,
        sql: &mut String,
        params: &mut Vec<&'query dyn ToSql>,
    ) {
        if self.values.is_empty() {
            return AllQuery.to_sql(score, sql, params);
        }

        if score {
            write!(
                sql,
                "SELECT term_0.document_id AS document_id, {} * (term_0.score",
                self.boost
            )
            .unwrap();

            for idx in 1..self.values.len() {
                write!(sql, " + term_{idx}.score").unwrap();
            }

            write!(sql, ") AS score, {} AS terms FROM", self.values.len()).unwrap();
        } else {
            sql.push_str("SELECT term_0.document_id AS document_id FROM");
        }

        sql.push_str("(SELECT canter_postings.document_id AS document_id, canter_postings.position AS position");

        if score {
            write!(sql, ",\ncanter_bm25({}, {}, canter_terms.count, COUNT(canter_postings.position), canter_documents.count) AS score", self.documents, self.avg_documents_count).unwrap();
        }

        sql.push_str(
            "\nFROM canter_terms JOIN canter_postings ON canter_terms.id = canter_postings.term_id",
        );

        if score {
            sql.push_str("\nJOIN canter_documents ON canter_terms.field_id = canter_documents.field_id AND canter_postings.document_id = canter_documents.document_id");
        }

        write!(
            sql,
            r#"
WHERE canter_terms.field_id = {} AND canter_terms.value = ?
GROUP BY canter_postings.term_id, canter_postings.document_id) AS term_0"#,
            self.field_id
        )
        .unwrap();

        params.push(&self.values[0]);

        for idx in 1..self.values.len() {
            sql.push_str("\nJOIN (SELECT canter_postings.document_id AS document_id, canter_postings.position AS position");

            if score {
                write!(sql, ",\ncanter_bm25({}, {}, canter_terms.count, COUNT(canter_postings.position), canter_documents.count) AS score", self.documents, self.avg_documents_count).unwrap();
            }

            sql.push_str("\nFROM canter_terms JOIN canter_postings ON canter_terms.id = canter_postings.term_id");

            if score {
                sql.push_str("\nJOIN canter_documents ON canter_terms.field_id = canter_documents.field_id AND canter_postings.document_id = canter_documents.document_id");
            }

            write!(
                sql,
                r#"
WHERE canter_terms.field_id = {} AND canter_terms.value = ?
GROUP BY canter_postings.term_id, canter_postings.document_id) AS term_{idx}
ON term_{idx}.document_id = term_0.document_id AND term_{idx}.position - term_0.position = {idx}"#,
                self.field_id
            )
            .unwrap();

            params.push(&self.values[idx]);
        }
    }
}

#[derive(Clone, Copy)]
pub enum Occur {
    Should,
    Must,
    MustNot,
}

pub struct CombinedQuery {
    should: Vec<Box<dyn Query>>,
    must: Vec<Box<dyn Query>>,
    must_not: Vec<Box<dyn Query>>,
}

impl CombinedQuery {
    pub fn new<C>(clauses: C) -> Self
    where
        C: IntoIterator<Item = (Occur, Box<dyn Query>)>,
    {
        let mut should = Vec::new();
        let mut must = Vec::new();
        let mut must_not = Vec::new();

        for (occur, clause) in clauses {
            match occur {
                Occur::Should => should.push(clause),
                Occur::Must => must.push(clause),
                Occur::MustNot => must_not.push(clause),
            }
        }

        Self {
            should,
            must,
            must_not,
        }
    }
}

impl Query for CombinedQuery {
    fn to_sql<'query>(
        &'query self,
        score: bool,
        sql: &mut String,
        params: &mut Vec<&'query dyn ToSql>,
    ) {
        let clauses = self.must.len() + self.should.len();

        if clauses != 0 {
            if !self.must.is_empty() {
                sql.push_str("SELECT\nclause_0.document_id AS document_id");
            } else {
                sql.push_str("SELECT\nCOALESCE(NULL, clause_0.document_id");

                for idx in 1..clauses {
                    write!(sql, ", clause_{idx}.document_id").unwrap();
                }

                sql.push_str(") AS document_id");
            }

            if score {
                sql.push_str(",\n(IFNULL(clause_0.terms, 0)");

                for idx in 1..clauses {
                    write!(sql, " + IFNULL(clause_{idx}.terms, 0)").unwrap();
                }

                sql.push_str(") * (IFNULL(clause_0.score, 0)");

                for idx in 1..clauses {
                    write!(sql, " + IFNULL(clause_{idx}.score, 0)").unwrap();
                }

                sql.push_str(") AS score,\n1 as terms");
            }

            sql.push_str("\nFROM");

            if !self.must.is_empty() {
                sql.push_str("\n(");

                self.must[0].to_sql(score, sql, params);

                sql.push_str(") AS clause_0");

                for idx in 1..self.must.len() {
                    sql.push_str("\nJOIN (");

                    self.must[idx].to_sql(score, sql, params);

                    write!(sql, ") AS clause_{idx} USING (document_id)").unwrap();
                }
            }

            if !self.should.is_empty() {
                if !self.must.is_empty() {
                    sql.push_str("\nLEFT JOIN (")
                } else {
                    sql.push_str("\n(");
                }

                self.should[0].to_sql(score, sql, params);

                write!(sql, ") AS clause_{}", self.must.len()).unwrap();
                if !self.must.is_empty() {
                    sql.push_str(" USING (document_id)")
                }

                for idx in 1..self.should.len() {
                    sql.push_str("\nFULL JOIN (");

                    self.should[idx].to_sql(score, sql, params);

                    write!(
                        sql,
                        ") AS clause_{} USING (document_id)",
                        self.must.len() + idx
                    )
                    .unwrap();
                }
            }
        } else {
            AllQuery.to_sql(score, sql, params);
        }

        if !self.must_not.is_empty() {
            for idx in 0..self.must_not.len() {
                sql.push_str("\nLEFT JOIN (");

                self.must_not[idx].to_sql(false, sql, params);

                write!(sql, ") AS clause_{} USING (document_id)", clauses + idx).unwrap();
            }

            write!(sql, "\nWHERE clause_{}.document_id IS NULL", clauses).unwrap();

            for idx in 1..self.must_not.len() {
                write!(sql, " AND clause_{}.document_id IS NULL", clauses + idx).unwrap();
            }
        }
    }
}
