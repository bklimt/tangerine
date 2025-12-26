use fjall::Config;

use crate::{
    error::Error,
    store::{DocumentStore, PostingListStore, TermStore},
};

pub type DocumentId = u128;

#[derive(Debug)]
pub struct DocumentData {
    pub length: u64,
}

impl DocumentData {
    pub fn zero() -> Self {
        DocumentData { length: 0 }
    }
}

#[derive(Debug)]
pub struct TermData {
    pub count: u64,          // total number of times this term occurred
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

#[derive(Debug)]
pub struct DocumentTermData {
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
    fn search(&self, terms: &[String]) -> Result<Vec<DocumentId>, Error> {
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
        }

        Ok(vec![])
    }
}
