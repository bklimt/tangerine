use brotopuf::{Deserialize, DeserializeField, Serialize};
use fjall::Config;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;

use crate::{
    error::Error,
    store::{DocumentStore, PostingListStore, TermStore},
};

pub type DocumentId = u128;

#[derive(Debug, Serialize, Deserialize)]
pub struct DocumentData {
    #[id(0)]
    pub length: u64,
}

impl DocumentData {
    pub fn zero() -> Self {
        DocumentData { length: 0 }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TermData {
    #[id(0)]
    pub count: u64, // total number of times this term occurred

    #[id(1)]
    pub document_count: u64, // total number of documents this term occurred in
}

impl TermData {
    pub fn zero() -> Self {
        TermData {
            count: 0,
            document_count: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DocumentTermData {
    #[id(0)]
    pub count: u64, // the number of times this term occurs in this doc
}

impl DocumentTermData {
    pub fn zero() -> Self {
        DocumentTermData { count: 0 }
    }
}

// An inverted index is a map of string token to posting list.
// The token is the word being looked up, and the posting list
// is a list of all the documents that the word is in, sorted
// by their unique id. The entry also contains metadata about
// the word, such as where in the document the word appeared.
struct InvertedIndex {
    terms: TermStore,
    docs: DocumentStore,
    postings: PostingListStore,
}

trait Scorer {
    fn score(
        &self,
        doc_data: &DocumentData,
        term_data: &Vec<TermData>,
        doc_term_data: &Vec<DocumentTermData>,
    ) -> f32;
}

impl InvertedIndex {
    pub fn new(path: &str) -> Result<Self, Error> {
        let keyspace = Config::new(path).open()?;
        let terms = TermStore::with_keyspace(&keyspace)?;
        let docs = DocumentStore::with_keyspace(&keyspace)?;
        let postings = PostingListStore::with_keyspace(&keyspace)?;
        Ok(InvertedIndex {
            terms,
            docs,
            postings,
        })
    }

    // A search where all terms are required.
    fn search(
        &self,
        terms: &[String],
        scorer: impl Scorer,
        max_docs: i32,
    ) -> Result<Vec<DocumentId>, Error> {
        // Look up the data for each term.
        let term_data: Result<Vec<Option<TermData>>, Error> =
            terms.iter().map(|term| self.terms.get(term)).collect();
        let term_data = term_data?;
        let term_data: Vec<TermData> = term_data
            .into_iter()
            .map(|item| item.unwrap_or(TermData::zero()))
            .collect();

        // Look up all the posting lists.
        let mut postings: Vec<_> = terms
            .iter()
            .map(|term| self.postings.get(term).peekable())
            .collect();

        let mut top_docs = PriorityQueue::new();

        loop {
            // Find the lowest id doc to score.
            let mut first_doc: Option<u128> = None;
            for posting in postings.iter_mut() {
                if let Some(result) = posting.peek() {
                    match result {
                        Ok((id, _data)) => {
                            if let Some(lowest_id) = first_doc {
                                if *id < lowest_id {
                                    first_doc = Some(*id)
                                }
                            } else {
                                first_doc = Some(*id)
                            }
                        }
                        Err(_) => return Err(posting.next().unwrap().unwrap_err()),
                    }
                }
            }
            let Some(first_doc) = first_doc else {
                break;
            };

            // Grab the data for that doc.
            let doc_data = self.docs.get(first_doc)?.unwrap_or(DocumentData::zero());

            // Grab the data for each term in this doc.
            let mut doc_term_data: Vec<DocumentTermData> = Vec::new();
            for posting in postings.iter_mut() {
                if let Some(result) = posting.peek() {
                    match result {
                        Ok((id, _data)) => {
                            if *id == first_doc {
                                let (_, data) = posting.next().unwrap().unwrap();
                                doc_term_data.push(data);
                            } else {
                                doc_term_data.push(DocumentTermData::zero());
                            }
                        }
                        Err(_) => return Err(posting.next().unwrap().unwrap_err()),
                    }
                }
            }

            let score = scorer.score(&doc_data, &term_data, &doc_term_data);
            top_docs.push(first_doc, OrderedFloat(-score));
            if top_docs.len() as i32 > max_docs {
                top_docs.pop();
            }
        }

        let results = top_docs.iter().rev().map(|(id, _score)| *id).collect();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct TestScorer {}

    impl Scorer for TestScorer {
        fn score(
            &self,
            doc_data: &DocumentData,
            term_data: &Vec<TermData>,
            doc_term_data: &Vec<DocumentTermData>,
        ) -> f32 {
            if doc_data.length == 101 {
                for term_data in doc_term_data {
                    // TODO: Check this.
                }
            } else if doc_data.length == 201 {
                for term_data in doc_term_data {
                    // TODO: Check this.
                }
            } else {
                assert!(false, "unknown document {}", doc_data.length);
            }

            for term in term_data {
                // TODO: Pass the term in here.
            }

            // TODO: Implement something better here.
            return doc_data.length as f32;
        }
    }

    // TODO: Add tests where various entries are missing.
    // TODO: Add tests for max doc limit.
    #[test]
    fn test_search() -> Result<(), Error> {
        let terms: HashMap<String, TermData> = [
            (
                "a".to_string(),
                TermData {
                    count: 1,
                    document_count: 2,
                },
            ),
            (
                "b".to_string(),
                TermData {
                    count: 3,
                    document_count: 4,
                },
            ),
        ]
        .into_iter()
        .collect();

        let documents: HashMap<u128, DocumentData> = [
            (100, DocumentData { length: 101 }),
            (200, DocumentData { length: 201 }),
        ]
        .into_iter()
        .collect();

        let document_term_data: HashMap<(String, u128), DocumentTermData> = [
            (("a".to_string(), 100), DocumentTermData { count: 5 }),
            (("b".to_string(), 100), DocumentTermData { count: 6 }),
            (("a".to_string(), 200), DocumentTermData { count: 7 }),
            (("b".to_string(), 200), DocumentTermData { count: 8 }),
        ]
        .into_iter()
        .collect();

        let index = InvertedIndex::new("/tmp/tangerine/search_tests")?;

        for (term, data) in terms.iter() {
            index.terms.put(&term, data)?;
        }
        for (doc, data) in documents.iter() {
            index.docs.put(*doc, data)?;
        }
        for ((term, doc), data) in document_term_data.iter() {
            index.postings.put(&term, *doc, data)?;
        }

        let scorer = TestScorer {};
        let query: Vec<String> = ["a", "b"].into_iter().map(|s| s.to_string()).collect();
        let results = index.search(&query, scorer, 10)?;

        assert_eq!(2, results.len());
        assert_eq!(200, *results.get(0).unwrap());
        assert_eq!(100, *results.get(1).unwrap());

        Ok(())
    }
}
