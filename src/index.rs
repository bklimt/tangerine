// Metadata about one occurrence of a token in a document.
pub struct Occurrence {
    pub position: usize,
    pub line: usize,
    pub column: usize,
    pub offset: usize,
    pub partial: bool,
}

// On
struct PostingListEntry {
    document_id: u128,

    // Where the token appeared in the doc, sorted by position.
    positions: Vec<Occurrence>,
}

// An inverted index is a map of string token to posting list.
// The token is the word being looked up, and the posting list
// is a list of all the documents that the word is in, sorted
// by their unique id. The entry also contains metadata about
// the word, such as where in the document the word appeared.
struct InvertedIndex {}
