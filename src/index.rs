use std::collections::HashMap;

use brotopuf::{Deserialize, DeserializeField, Serialize};
use fjall::Keyspace;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;

use crate::{
    error::Error,
    parse::{TokenProcessor, TokenSlice, parse_text},
    store::{DocumentStore, IndexStore, PostingListStore, TermStore},
};

pub type DocumentId = u128;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DocumentData {
    #[id(0)]
    pub path: String,

    #[id(1)]
    pub length: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TermData {
    #[id(0)]
    pub count: u64, // total number of times this term occurred

    #[id(1)]
    pub document_count: u64, // total number of documents this term occurred in
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DocumentTermData {
    #[id(0)]
    pub body_count: u64, // the number of times this term occurs in this doc

    #[id(1)]
    pub path_count: u64, // the number of times this term occurs in this doc's path
}

struct DocProcessor {
    id: DocumentId,
    path: String,
    in_path: bool,
    length: u64,
    terms: HashMap<String, TermData>,
    doc_terms: HashMap<String, DocumentTermData>,
}

impl DocProcessor {
    fn new(id: DocumentId, path: &str) -> Self {
        DocProcessor {
            id,
            path: path.to_string(),
            in_path: false,
            length: 0,
            terms: HashMap::new(),
            doc_terms: HashMap::new(),
        }
    }

    fn finalize(&self, index: &IndexStore) -> Result<(), Error> {
        let doc_data = DocumentData {
            path: self.path.clone(),
            length: self.length,
        };
        index.documents().put(self.id, &doc_data)?;

        for (term, term_data) in self.terms.iter() {
            index.terms().put(term, term_data)?;
        }

        for (term, doc_term_data) in self.doc_terms.iter() {
            index.posting_lists().put(term, self.id, doc_term_data)?;
        }

        Ok(())
    }
}

impl TokenProcessor for DocProcessor {
    fn process_token(&mut self, token: &TokenSlice) {
        // TODO: Deal with partial matches.
        let mut term_data = self.terms.remove(token.token).unwrap_or_default();
        let mut doc_term_data = self.doc_terms.remove(token.token).unwrap_or_default();
        term_data.count += 1;
        term_data.document_count = 1;
        if self.in_path {
            doc_term_data.path_count += 1;
        } else {
            doc_term_data.body_count += 1;
        }
        self.terms.insert(token.token.to_string(), term_data);
        self.doc_terms
            .insert(token.token.to_string(), doc_term_data);

        self.length = self.length.max((token.occurrence.position + 1) as u64);
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

    fn new_document_id(&self) -> Result<DocumentId, Error> {
        self.store.documents().new_id()
    }

    // Add a document to the index.
    // If the document is already in the index, this will add it a second itme.
    pub fn add_document(
        &self,
        path: &str,
        doc: &mut impl std::io::Read,
    ) -> Result<DocumentId, Error> {
        let id = self.new_document_id()?;
        let mut processor = DocProcessor::new(id, path);
        let mut text = String::new();
        processor.in_path = true;
        parse_text(path, &mut processor);
        processor.in_path = false;
        doc.read_to_string(&mut text)?;
        parse_text(&text, &mut processor);
        processor.finalize(&self.store)?;
        Ok(id)
    }

    // Search for docs that have any of the words.
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
            .map(|item| item.unwrap_or(TermData::default()))
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
            let doc_data = self
                .docs()
                .get(first_doc)?
                .unwrap_or(DocumentData::default());

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
                                doc_term_data.push(DocumentTermData::default());
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
                    assert_eq!("/one/hundred", doc_data.path);
                    assert_eq!(101, doc_data.length);
                    for (term, doc_term_data) in zip(terms, doc_term_data) {
                        match term.as_str() {
                            "a" => {
                                assert_eq!(15, doc_term_data.path_count);
                                assert_eq!(5, doc_term_data.body_count);
                            }
                            "b" => {
                                assert_eq!(16, doc_term_data.path_count);
                                assert_eq!(6, doc_term_data.body_count);
                            }
                            _ => {
                                assert!(false, "unknown term {}", term);
                            }
                        }
                    }
                }
                200 => {
                    assert_eq!("/two/hundred", doc_data.path);
                    assert_eq!(201, doc_data.length);
                    for (term, doc_term_data) in zip(terms, doc_term_data) {
                        match term.as_str() {
                            "a" => {
                                assert_eq!(17, doc_term_data.path_count);
                                assert_eq!(7, doc_term_data.body_count);
                            }
                            "b" => {
                                assert_eq!(18, doc_term_data.path_count);
                                assert_eq!(8, doc_term_data.body_count);
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
            (
                100,
                DocumentData {
                    path: "/one/hundred".to_string(),
                    length: 101,
                },
            ),
            (
                200,
                DocumentData {
                    path: "/two/hundred".to_string(),
                    length: 201,
                },
            ),
        ]
        .into_iter()
        .collect();

        let document_term_data: HashMap<(String, u128), DocumentTermData> = [
            (
                ("a".to_string(), 100),
                DocumentTermData {
                    body_count: 5,
                    path_count: 15,
                },
            ),
            (
                ("b".to_string(), 100),
                DocumentTermData {
                    body_count: 6,
                    path_count: 16,
                },
            ),
            (
                ("a".to_string(), 200),
                DocumentTermData {
                    body_count: 7,
                    path_count: 17,
                },
            ),
            (
                ("b".to_string(), 200),
                DocumentTermData {
                    body_count: 8,
                    path_count: 18,
                },
            ),
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
            (
                100,
                DocumentData {
                    path: "/hundred/one".to_string(),
                    length: 5,
                },
            ),
            (
                200,
                DocumentData {
                    path: "/hundred/two".to_string(),
                    length: 2,
                },
            ),
            (
                300,
                DocumentData {
                    path: "/hundred/three".to_string(),
                    length: 4,
                },
            ),
            (
                400,
                DocumentData {
                    path: "/hundred/four".to_string(),
                    length: 3,
                },
            ),
            (
                500,
                DocumentData {
                    path: "/hundred/five".to_string(),
                    length: 1,
                },
            ),
            (
                600,
                DocumentData {
                    path: "/hundred/six".to_string(),
                    length: 6,
                },
            ),
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
            (("a".to_string(), 100), DocumentTermData::default()),
            (("a".to_string(), 200), DocumentTermData::default()),
            (("a".to_string(), 300), DocumentTermData::default()),
            (("a".to_string(), 400), DocumentTermData::default()),
            (("a".to_string(), 500), DocumentTermData::default()),
            (("a".to_string(), 600), DocumentTermData::default()),
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
            (
                100,
                DocumentData {
                    path: "/hundred/one".to_string(),
                    length: 5,
                },
            ),
            (
                200,
                DocumentData {
                    path: "/hundred/two".to_string(),
                    length: 2,
                },
            ),
            (
                300,
                DocumentData {
                    path: "/hundred/three".to_string(),
                    length: 4,
                },
            ),
            (
                400,
                DocumentData {
                    path: "/hundred/four".to_string(),
                    length: 3,
                },
            ),
            (
                500,
                DocumentData {
                    path: "/hundred/five".to_string(),
                    length: 1,
                },
            ),
            (
                600,
                DocumentData {
                    path: "/hundred/six".to_string(),
                    length: 6,
                },
            ),
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
            (("a".to_string(), 100), DocumentTermData::default()),
            (("a".to_string(), 200), DocumentTermData::default()),
            (("a".to_string(), 300), DocumentTermData::default()),
            (("a".to_string(), 400), DocumentTermData::default()),
            (("a".to_string(), 500), DocumentTermData::default()),
            (("a".to_string(), 600), DocumentTermData::default()),
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
            (
                100,
                DocumentData {
                    path: "/hundred/one".to_string(),
                    length: 5,
                },
            ),
            (
                200,
                DocumentData {
                    path: "/hundred/two".to_string(),
                    length: 2,
                },
            ),
            (
                300,
                DocumentData {
                    path: "/hundred/three".to_string(),
                    length: 4,
                },
            ),
            (
                400,
                DocumentData {
                    path: "/hundred/four".to_string(),
                    length: 3,
                },
            ),
            (
                500,
                DocumentData {
                    path: "/hundred/five".to_string(),
                    length: 1,
                },
            ),
            (
                600,
                DocumentData {
                    path: "/hundred/six".to_string(),
                    length: 6,
                },
            ),
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
            (("a".to_string(), 100), DocumentTermData::default()),
            (("a".to_string(), 200), DocumentTermData::default()),
            (("a".to_string(), 300), DocumentTermData::default()),
            (("a".to_string(), 400), DocumentTermData::default()),
            (("a".to_string(), 500), DocumentTermData::default()),
            (("a".to_string(), 600), DocumentTermData::default()),
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
            (
                100,
                DocumentData {
                    path: "/hundred/one".to_string(),
                    length: 5,
                },
            ),
            (
                200,
                DocumentData {
                    path: "/hundred/two".to_string(),
                    length: 2,
                },
            ),
            (
                300,
                DocumentData {
                    path: "/hundred/three".to_string(),
                    length: 4,
                },
            ),
            (
                400,
                DocumentData {
                    path: "/hundred/four".to_string(),
                    length: 3,
                },
            ),
            (
                500,
                DocumentData {
                    path: "/hundred/five".to_string(),
                    length: 1,
                },
            ),
            (
                600,
                DocumentData {
                    path: "/hundred/six".to_string(),
                    length: 6,
                },
            ),
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
            (("a".to_string(), 100), DocumentTermData::default()),
            (("a".to_string(), 200), DocumentTermData::default()),
            (("a".to_string(), 300), DocumentTermData::default()),
            (("a".to_string(), 400), DocumentTermData::default()),
            (("a".to_string(), 500), DocumentTermData::default()),
            (("a".to_string(), 600), DocumentTermData::default()),
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
