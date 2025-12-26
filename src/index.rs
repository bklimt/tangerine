use fjall::Config;

use crate::{
    error::Error,
    store::{DocumentStore, PostingListStore, TermStore},
};

pub type DocumentId = u128;

pub struct DocumentData {
    pub length: u64,
}

pub struct TermData {
    pub count: u64,          // total number of times this term occurred
    pub document_count: u64, // total number of documents this term occurred in
}

pub struct DocumentTermData {
    pub count: u64, // the number of times this term occurs in this doc
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
        let keyspace = Config::new("/tmp/tangerine/testdata").open()?;
        let terms = TermStore::with_keyspace(&keyspace)?;
        let docs = DocumentStore::with_keyspace(&keyspace)?;
        let postings = PostingListStore::with_keyspace(&keyspace)?;
        Ok(InvertedIndex {
            terms,
            docs,
            postings,
        })
    }

    fn search() {}
}
