use brotopuf::{Deserialize, DeserializeField, Serialize};
use fjall::Keyspace;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;

use crate::{
    error::Error,
    store::{DocumentStore, IndexStore, PostingListStore, TermStore},
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
pub struct InvertedIndex {
    store: IndexStore,
}

pub trait Scorer {
    fn score(
        &self,
        doc_id: DocumentId,
        doc_data: &DocumentData,
        terms: &[String],
        term_data: &[TermData],
        doc_term_data: &[DocumentTermData],
    ) -> f32;
}

impl InvertedIndex {
    pub fn new(keyspace: &Keyspace) -> Result<Self, Error> {
        let store = IndexStore::new(keyspace)?;
        Ok(InvertedIndex { store })
    }

    fn terms(&self) -> &TermStore {
        self.store.terms()
    }

    fn docs(&self) -> &DocumentStore {
        self.store.documents()
    }

    fn postings(&self) -> &PostingListStore {
        self.store.posting_lists()
    }

    // A search where all terms are required.
    pub fn search(
        &self,
        terms: &[String],
        scorer: impl Scorer,
        max_docs: i32,
    ) -> Result<Vec<DocumentId>, Error> {
        // Look up the data for each term.
        let term_data: Result<Vec<Option<TermData>>, Error> =
            terms.iter().map(|term| self.terms().get(term)).collect();
        let term_data = term_data?;
        let term_data: Vec<TermData> = term_data
            .into_iter()
            .map(|item| item.unwrap_or(TermData::zero()))
            .collect();

        // Look up all the posting lists.
        let mut postings: Vec<_> = terms
            .iter()
            .map(|term| self.postings().get(term).peekable())
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
            let doc_data = self.docs().get(first_doc)?.unwrap_or(DocumentData::zero());

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

            let score = scorer.score(first_doc, &doc_data, &terms, &term_data, &doc_term_data);
            top_docs.push(first_doc, OrderedFloat(-score));
            if top_docs.len() as i32 > max_docs {
                top_docs.pop();
            }
        }

        let results: Vec<(u128, OrderedFloat<f32>)> = top_docs.into_sorted_iter().collect();
        /*
        println!("results:");
        for (id, score) in results.iter().rev() {
            println!("  doc: {}, score: {}", *id, *score);
        }
        */
        let results = results.into_iter().rev().map(|(id, _score)| id).collect();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use fjall::Config;

    use super::*;
    use std::{collections::HashMap, iter::zip};

    // A test scorer that verifies all the arguments thoroughly and then just sorts by document id.
    struct ArgumentVerifyingTestScorer {}

    impl Scorer for ArgumentVerifyingTestScorer {
        fn score(
            &self,
            doc_id: DocumentId,
            doc_data: &DocumentData,
            terms: &[String],
            term_data: &[TermData],
            doc_term_data: &[DocumentTermData],
        ) -> f32 {
            assert_eq!(terms.len(), term_data.len());
            assert_eq!(term_data.len(), doc_term_data.len());

            for (term, term_data) in zip(terms, term_data) {
                match term.as_str() {
                    "a" => {
                        assert_eq!(1, term_data.count);
                        assert_eq!(2, term_data.document_count);
                    }
                    "b" => {
                        assert_eq!(3, term_data.count);
                        assert_eq!(4, term_data.document_count);
                    }
                    _ => {
                        assert!(false, "unknown term {}", term);
                    }
                }
            }

            match doc_id {
                100 => {
                    assert_eq!(101, doc_data.length);
                    for (term, doc_term_data) in zip(terms, doc_term_data) {
                        match term.as_str() {
                            "a" => {
                                assert_eq!(5, doc_term_data.count);
                            }
                            "b" => {
                                assert_eq!(6, doc_term_data.count);
                            }
                            _ => {
                                assert!(false, "unknown term {}", term);
                            }
                        }
                    }
                }
                200 => {
                    assert_eq!(201, doc_data.length);
                    for (term, doc_term_data) in zip(terms, doc_term_data) {
                        match term.as_str() {
                            "a" => {
                                assert_eq!(7, doc_term_data.count);
                            }
                            "b" => {
                                assert_eq!(8, doc_term_data.count);
                            }
                            _ => {
                                assert!(false, "unknown term {}", term);
                            }
                        }
                    }
                }
                _ => {
                    assert!(false, "unknown document {}", doc_id);
                }
            }

            return doc_id as f32;
        }
    }

    #[test]
    fn test_search_arguments_passed_to_scorer() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_search_arguments_passed_to_scorer")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)
            .unwrap()
            .delete(&keyspace)
            .unwrap();

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

        let index = InvertedIndex::new(&keyspace)?;

        for (term, data) in terms.iter() {
            index.terms().put(&term, data)?;
        }
        for (doc, data) in documents.iter() {
            index.docs().put(*doc, data)?;
        }
        for ((term, doc), data) in document_term_data.iter() {
            index.postings().put(&term, *doc, data)?;
        }

        let scorer = ArgumentVerifyingTestScorer {};
        let query: Vec<String> = ["a", "b"].into_iter().map(|s| s.to_string()).collect();
        let results = index.search(&query, scorer, 10)?;

        assert_eq!(2, results.len());
        assert_eq!(200, *results.get(0).unwrap());
        assert_eq!(100, *results.get(1).unwrap());

        Ok(())
    }

    // A test scorer that just uses doc_data.length to sort the documents.
    struct SortingScorer {}

    impl Scorer for SortingScorer {
        fn score(
            &self,
            _doc_id: DocumentId,
            doc_data: &DocumentData,
            _terms: &[String],
            _term_data: &[TermData],
            _doc_term_data: &[DocumentTermData],
        ) -> f32 {
            doc_data.length as f32
        }
    }

    #[test]
    fn test_search_results_are_sorted_by_score() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_search_results_are_sorted_by_score")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)
            .unwrap()
            .delete(&keyspace)
            .unwrap();

        let documents: HashMap<u128, DocumentData> = [
            (100, DocumentData { length: 5 }),
            (200, DocumentData { length: 2 }),
            (300, DocumentData { length: 4 }),
            (400, DocumentData { length: 3 }),
            (500, DocumentData { length: 1 }),
            (600, DocumentData { length: 6 }),
        ]
        .into_iter()
        .collect();

        let terms: HashMap<String, TermData> = [(
            "a".to_string(),
            TermData {
                count: 1,
                document_count: 2,
            },
        )]
        .into_iter()
        .collect();

        let document_term_data: HashMap<(String, u128), DocumentTermData> = [
            (("a".to_string(), 100), DocumentTermData { count: 0 }),
            (("a".to_string(), 200), DocumentTermData { count: 0 }),
            (("a".to_string(), 300), DocumentTermData { count: 0 }),
            (("a".to_string(), 400), DocumentTermData { count: 0 }),
            (("a".to_string(), 500), DocumentTermData { count: 0 }),
            (("a".to_string(), 600), DocumentTermData { count: 0 }),
        ]
        .into_iter()
        .collect();

        let index = InvertedIndex::new(&keyspace)?;

        for (term, data) in terms.iter() {
            index.terms().put(&term, data)?;
        }
        for (doc, data) in documents.iter() {
            index.docs().put(*doc, data)?;
        }
        for ((term, doc), data) in document_term_data.iter() {
            index.postings().put(&term, *doc, data)?;
        }

        let scorer = SortingScorer {};
        let query: Vec<String> = ["a"].into_iter().map(|s| s.to_string()).collect();
        let results = index.search(&query, scorer, 10)?;

        assert_eq!(6, results.len());
        assert_eq!(600, *results.get(0).unwrap());
        assert_eq!(100, *results.get(1).unwrap());
        assert_eq!(300, *results.get(2).unwrap());
        assert_eq!(400, *results.get(3).unwrap());
        assert_eq!(200, *results.get(4).unwrap());
        assert_eq!(500, *results.get(5).unwrap());

        Ok(())
    }

    #[test]
    fn test_search_max_docs_works() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_search_max_docs_works")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)
            .unwrap()
            .delete(&keyspace)
            .unwrap();

        let documents: HashMap<u128, DocumentData> = [
            (100, DocumentData { length: 5 }),
            (200, DocumentData { length: 2 }),
            (300, DocumentData { length: 4 }),
            (400, DocumentData { length: 3 }),
            (500, DocumentData { length: 1 }),
            (600, DocumentData { length: 6 }),
        ]
        .into_iter()
        .collect();

        let terms: HashMap<String, TermData> = [(
            "a".to_string(),
            TermData {
                count: 1,
                document_count: 2,
            },
        )]
        .into_iter()
        .collect();

        let document_term_data: HashMap<(String, u128), DocumentTermData> = [
            (("a".to_string(), 100), DocumentTermData { count: 0 }),
            (("a".to_string(), 200), DocumentTermData { count: 0 }),
            (("a".to_string(), 300), DocumentTermData { count: 0 }),
            (("a".to_string(), 400), DocumentTermData { count: 0 }),
            (("a".to_string(), 500), DocumentTermData { count: 0 }),
            (("a".to_string(), 600), DocumentTermData { count: 0 }),
        ]
        .into_iter()
        .collect();

        let index = InvertedIndex::new(&keyspace)?;

        for (term, data) in terms.iter() {
            index.terms().put(&term, data)?;
        }
        for (doc, data) in documents.iter() {
            index.docs().put(*doc, data)?;
        }
        for ((term, doc), data) in document_term_data.iter() {
            index.postings().put(&term, *doc, data)?;
        }

        let scorer = SortingScorer {};
        let query: Vec<String> = ["a"].into_iter().map(|s| s.to_string()).collect();
        let results = index.search(&query, scorer, 3)?;

        assert_eq!(3, results.len());
        assert_eq!(600, *results.get(0).unwrap());
        assert_eq!(100, *results.get(1).unwrap());
        assert_eq!(300, *results.get(2).unwrap());

        Ok(())
    }

    #[test]
    fn test_search_with_term_not_id_docs() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_search_with_term_not_id_docs")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)
            .unwrap()
            .delete(&keyspace)
            .unwrap();

        let documents: HashMap<u128, DocumentData> = [
            (100, DocumentData { length: 5 }),
            (200, DocumentData { length: 2 }),
            (300, DocumentData { length: 4 }),
            (400, DocumentData { length: 3 }),
            (500, DocumentData { length: 1 }),
            (600, DocumentData { length: 6 }),
        ]
        .into_iter()
        .collect();

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

        let document_term_data: HashMap<(String, u128), DocumentTermData> = [
            (("a".to_string(), 100), DocumentTermData { count: 0 }),
            (("a".to_string(), 200), DocumentTermData { count: 0 }),
            (("a".to_string(), 300), DocumentTermData { count: 0 }),
            (("a".to_string(), 400), DocumentTermData { count: 0 }),
            (("a".to_string(), 500), DocumentTermData { count: 0 }),
            (("a".to_string(), 600), DocumentTermData { count: 0 }),
        ]
        .into_iter()
        .collect();

        let index = InvertedIndex::new(&keyspace)?;

        for (term, data) in terms.iter() {
            index.terms().put(&term, data)?;
        }
        for (doc, data) in documents.iter() {
            index.docs().put(*doc, data)?;
        }
        for ((term, doc), data) in document_term_data.iter() {
            index.postings().put(&term, *doc, data)?;
        }

        let scorer = SortingScorer {};
        let query: Vec<String> = ["a", "b"].into_iter().map(|s| s.to_string()).collect();
        let results = index.search(&query, scorer, 10)?;

        assert_eq!(6, results.len());
        assert_eq!(600, *results.get(0).unwrap());
        assert_eq!(100, *results.get(1).unwrap());
        assert_eq!(300, *results.get(2).unwrap());
        assert_eq!(400, *results.get(3).unwrap());
        assert_eq!(200, *results.get(4).unwrap());
        assert_eq!(500, *results.get(5).unwrap());

        Ok(())
    }

    #[test]
    fn test_search_with_nonexistent_term() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_search_with_nonexistent_term")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)
            .unwrap()
            .delete(&keyspace)
            .unwrap();

        let documents: HashMap<u128, DocumentData> = [
            (100, DocumentData { length: 5 }),
            (200, DocumentData { length: 2 }),
            (300, DocumentData { length: 4 }),
            (400, DocumentData { length: 3 }),
            (500, DocumentData { length: 1 }),
            (600, DocumentData { length: 6 }),
        ]
        .into_iter()
        .collect();

        let terms: HashMap<String, TermData> = [(
            "a".to_string(),
            TermData {
                count: 1,
                document_count: 2,
            },
        )]
        .into_iter()
        .collect();

        let document_term_data: HashMap<(String, u128), DocumentTermData> = [
            (("a".to_string(), 100), DocumentTermData { count: 0 }),
            (("a".to_string(), 200), DocumentTermData { count: 0 }),
            (("a".to_string(), 300), DocumentTermData { count: 0 }),
            (("a".to_string(), 400), DocumentTermData { count: 0 }),
            (("a".to_string(), 500), DocumentTermData { count: 0 }),
            (("a".to_string(), 600), DocumentTermData { count: 0 }),
        ]
        .into_iter()
        .collect();

        let index = InvertedIndex::new(&keyspace)?;

        for (term, data) in terms.iter() {
            index.terms().put(&term, data)?;
        }
        for (doc, data) in documents.iter() {
            index.docs().put(*doc, data)?;
        }
        for ((term, doc), data) in document_term_data.iter() {
            index.postings().put(&term, *doc, data)?;
        }

        let scorer = SortingScorer {};
        let query: Vec<String> = ["a", "b"].into_iter().map(|s| s.to_string()).collect();
        let results = index.search(&query, scorer, 10)?;

        assert_eq!(6, results.len());
        assert_eq!(600, *results.get(0).unwrap());
        assert_eq!(100, *results.get(1).unwrap());
        assert_eq!(300, *results.get(2).unwrap());
        assert_eq!(400, *results.get(3).unwrap());
        assert_eq!(200, *results.get(4).unwrap());
        assert_eq!(500, *results.get(5).unwrap());

        Ok(())
    }
}
