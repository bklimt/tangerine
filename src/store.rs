use crate::index::{DocumentId, DocumentTermData, TermData};
use crate::{error::Error, index::DocumentData};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};

pub struct TermStore {
    db: Partition,
}

impl TermStore {
    pub fn with_keyspace(keyspace: &Keyspace) -> Result<Self, Error> {
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("terms", options)?;
        Ok(TermStore { db })
    }

    pub fn get(&self, term: &str) -> Result<Option<TermData>, Error> {
        match self.db.get(term) {
            Ok(Some(slice)) => TermData::try_from(slice).map(|d| Some(d)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::FjallError(e)),
        }
    }

    pub fn put(&self, term: &str, data: &TermData) -> Result<(), Error> {
        self.db.insert(term, data).map_err(|e| Error::FjallError(e))
    }
}

impl TryFrom<Slice> for TermData {
    type Error = Error;

    fn try_from(value: Slice) -> Result<Self, Error> {
        let mut bytes = BytesMut::from(&value[..]);
        let count = bytes.try_get_u64()?;
        let document_count = bytes.try_get_u64()?;
        Ok(TermData {
            count,
            document_count,
        })
    }
}

impl From<&TermData> for Slice {
    fn from(value: &TermData) -> Self {
        let mut bytes = BytesMut::with_capacity(8 * 2);
        bytes.put_u64(value.count);
        bytes.put_u64(value.document_count);
        Slice::new(&bytes[..])
    }
}

pub struct DocumentStore {
    db: Partition,
}

impl DocumentStore {
    pub fn with_keyspace(keyspace: &Keyspace) -> Result<Self, Error> {
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("docs", options)?;
        Ok(DocumentStore { db })
    }

    pub fn get(&self, id: DocumentId) -> Result<Option<DocumentData>, Error> {
        match self.db.get(id.to_be_bytes()) {
            Ok(Some(slice)) => DocumentData::try_from(slice).map(|d| Some(d)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::FjallError(e)),
        }
    }

    pub fn put(&self, id: DocumentId, data: &DocumentData) -> Result<(), Error> {
        self.db
            .insert(id.to_be_bytes(), data)
            .map_err(|e| Error::FjallError(e))
    }
}

impl TryFrom<Slice> for DocumentData {
    type Error = Error;

    fn try_from(value: Slice) -> Result<Self, Error> {
        let mut bytes = BytesMut::from(&value[..]);
        let length = bytes.try_get_u64()?;
        Ok(DocumentData { length })
    }
}

impl From<&DocumentData> for Slice {
    fn from(value: &DocumentData) -> Self {
        let mut bytes = BytesMut::with_capacity(8 * 2);
        bytes.put_u64(value.length);
        Slice::new(&bytes[..])
    }
}

fn make_posting_list_key(term: &str, doc: DocumentId) -> Bytes {
    // Enough for a wide-unicode string term + a 128 bit id + a delimiter
    let mut buf = BytesMut::with_capacity(term.len() * 4 + 17);
    buf.put(term.as_bytes());
    buf.put(&[0u8][..]);
    buf.put(&doc.to_be_bytes()[..]);
    buf.freeze()
}

fn parse_posting_list_key(key: Slice) -> Result<(String, DocumentId), Error> {
    let mut buf = BytesMut::from(&key[..]);
    let length = buf.len();
    if length < 17 {
        return Err(Error::DeserializationError {});
    }
    let term_length = length - 17;
    let term = str::from_utf8(&buf[..term_length]).map_err(|e| Error::Utf8Error(e))?;
    let term = term.to_string();
    buf.advance(term_length);
    let delimiter = buf.get_u8();
    if delimiter != 0 {
        return Err(Error::DeserializationError {});
    }
    let doc_id = buf.get_u128();
    Ok((term, doc_id))
}

fn make_posting_list_prefix(term: &str) -> Bytes {
    // Enough for a wide-unicode string term + a 128 bit id + a delimiter
    let mut buf = BytesMut::with_capacity(term.len() * 4 + 1);
    buf.put(term.as_bytes());
    buf.put(&[0u8][..]);
    buf.freeze()
}

pub struct PostingListStore {
    db: Partition,
}

impl PostingListStore {
    pub fn with_keyspace(keyspace: &Keyspace) -> Result<Self, Error> {
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("postings", options)?;
        Ok(PostingListStore { db })
    }

    pub fn get(
        &self,
        term: &str,
    ) -> impl Iterator<Item = Result<(DocumentId, DocumentTermData), Error>> {
        let prefix = make_posting_list_prefix(term);
        self.db.prefix(prefix).map(|result| match result {
            Ok((key, data)) => {
                let (_, doc_id) = parse_posting_list_key(key)?;
                let data = data.try_into()?;
                Ok((doc_id, data))
            }
            Err(e) => Err(Error::FjallError(e)),
        })
    }

    pub fn put(
        &self,
        term: &str,
        document: DocumentId,
        data: &DocumentTermData,
    ) -> Result<(), Error> {
        let key = make_posting_list_key(term, document);
        self.db
            .insert(&key[..], data)
            .map_err(|e| Error::FjallError(e))
    }
}

impl TryFrom<Slice> for DocumentTermData {
    type Error = Error;

    fn try_from(value: Slice) -> Result<Self, Error> {
        let mut bytes = BytesMut::from(&value[..]);
        let count = bytes.try_get_u64()?;
        Ok(DocumentTermData { count })
    }
}

impl From<&DocumentTermData> for Slice {
    fn from(value: &DocumentTermData) -> Self {
        let mut bytes = BytesMut::with_capacity(8 * 2);
        bytes.put_u64(value.count);
        Slice::new(&bytes[..])
    }
}

#[cfg(test)]
mod tests {
    use fjall::Config;

    use super::*;

    #[test]
    fn test_term_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/testdata").open()?;
        let store = TermStore::with_keyspace(&keyspace)?;

        let term_data = TermData {
            count: 1,
            document_count: 2,
        };
        store.put("a", &term_data)?;

        let actual = store.get("a")?.unwrap();

        assert_eq!(1, actual.count);
        assert_eq!(2, actual.document_count);

        Ok(())
    }

    #[test]
    fn test_document_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/testdata").open()?;
        let store = DocumentStore::with_keyspace(&keyspace)?;

        let doc_data = DocumentData { length: 3 };
        store.put(123, &doc_data)?;

        let actual = store.get(123)?.unwrap();

        assert_eq!(3, actual.length);

        Ok(())
    }

    #[test]
    fn test_posting_list_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/testdata").open()?;
        let store = PostingListStore::with_keyspace(&keyspace)?;

        let doc_data = DocumentTermData { count: 4 };
        store.put("a", 1, &doc_data)?;

        let doc_data = DocumentTermData { count: 5 };
        store.put("a", 2, &doc_data)?;

        let result: Result<Vec<(DocumentId, DocumentTermData)>, Error> = store.get("a").collect();
        let result = result?;
        assert_eq!(2, result.len());

        let (result_doc, result_data) = result.get(0).unwrap();
        assert_eq!(1u128, *result_doc);
        assert_eq!(4, result_data.count);

        let (result_doc, result_data) = result.get(1).unwrap();
        assert_eq!(2u128, *result_doc);
        assert_eq!(5, result_data.count);

        Ok(())
    }
}
