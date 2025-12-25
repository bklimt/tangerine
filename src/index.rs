use bytes::{BufMut, Bytes, BytesMut};

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

fn make_posting_list_key(term: &str, doc: DocumentId) -> Bytes {
    // Enough for a wide-unicode string term + a 128 bit id + a delimiter
    let mut buf = BytesMut::with_capacity(term.len() * 4 + 17);
    buf.put(term.as_bytes());
    buf.put(&[0u8][..]);
    buf.put(&doc.to_be_bytes()[..]);
    buf.freeze()
}

fn make_posting_list_prefix(term: &str) -> Bytes {
    // Enough for a wide-unicode string term + a 128 bit id + a delimiter
    let mut buf = BytesMut::with_capacity(term.len() * 4 + 1);
    buf.put(term.as_bytes());
    buf.put(&[0u8][..]);
    buf.freeze()
}

// An inverted index is a map of string token to posting list.
// The token is the word being looked up, and the posting list
// is a list of all the documents that the word is in, sorted
// by their unique id. The entry also contains metadata about
// the word, such as where in the document the word appeared.
struct InvertedIndex {}

impl InvertedIndex {
    /*
    fn get(token: &str) -> impl Iterator<Item = Result<DocumentTermData, Error>> {

    }
    */
}
