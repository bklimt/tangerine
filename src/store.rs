use std::io::Cursor;

use crate::index::{DocumentId, DocumentTermData, TermData};
use crate::{error::Error, index::DocumentData};
use brotopuf::{Deserialize, Serialize};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fjall::{Keyspace, Partition, PartitionCreateOptions, Slice};

pub struct IndexStore {
    term_store: TermStore,
    document_store: DocumentStore,
    posting_list_store: PostingListStore,
}

impl IndexStore {
    pub fn new(keyspace: &Keyspace) -> Result<IndexStore, Error> {
        let term_store = TermStore::with_keyspace(&keyspace)?;
        let document_store = DocumentStore::with_keyspace(&keyspace)?;
        let posting_list_store = PostingListStore::with_keyspace(&keyspace)?;
        Ok(IndexStore {
            term_store,
            document_store,
            posting_list_store,
        })
    }

    pub fn terms(&self) -> &TermStore {
        &self.term_store
    }

    pub fn documents(&self) -> &DocumentStore {
        &self.document_store
    }

    pub fn posting_lists(&self) -> &PostingListStore {
        &self.posting_list_store
    }

    pub fn delete(self, keyspace: &Keyspace) -> Result<(), Error> {
        keyspace.delete_partition(self.term_store.db)?;
        keyspace.delete_partition(self.document_store.db)?;
        keyspace.delete_partition(self.posting_list_store.db)?;
        Ok(())
    }
}

pub struct TermStore {
    db: Partition,
}

impl TermStore {
    fn with_keyspace(keyspace: &Keyspace) -> Result<Self, Error> {
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("terms", options)?;
        Ok(TermStore { db })
    }

    pub fn get(&self, term: &str) -> Result<Option<TermData>, Error> {
        match self.db.get(term) {
            Ok(Some(slice)) => TermData::try_from(slice).map(|d| Some(d)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn put(&self, term: &str, data: &TermData) -> Result<(), Error> {
        Ok(self.db.insert(term, data)?)
    }
}

impl TryFrom<Slice> for TermData {
    type Error = Error;

    fn try_from(value: Slice) -> Result<Self, Error> {
        let mut buffer = Cursor::new(&value[..]);
        let mut term_data = TermData::zero();
        term_data.deserialize(buffer.get_mut())?;
        Ok(term_data)
    }
}

impl From<&TermData> for Slice {
    fn from(value: &TermData) -> Self {
        let mut buffer = Vec::new();
        value.serialize(&mut buffer).unwrap();
        Slice::new(&buffer[..])
    }
}

pub struct DocumentStore {
    db: Partition,
}

impl DocumentStore {
    fn with_keyspace(keyspace: &Keyspace) -> Result<Self, Error> {
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("docs", options)?;
        Ok(DocumentStore { db })
    }

    pub fn get(&self, id: DocumentId) -> Result<Option<DocumentData>, Error> {
        match self.db.get(id.to_be_bytes()) {
            Ok(Some(slice)) => DocumentData::try_from(slice).map(|d| Some(d)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn put(&self, id: DocumentId, data: &DocumentData) -> Result<(), Error> {
        Ok(self.db.insert(id.to_be_bytes(), data)?)
    }
}

impl TryFrom<Slice> for DocumentData {
    type Error = Error;

    fn try_from(value: Slice) -> Result<Self, Error> {
        let mut buffer = Cursor::new(&value[..]);
        let mut doc_data = DocumentData::zero();
        doc_data.deserialize(&mut buffer)?;
        Ok(doc_data)
    }
}

impl From<&DocumentData> for Slice {
    fn from(value: &DocumentData) -> Self {
        let mut buffer = Vec::new();
        value.serialize(&mut buffer).unwrap();
        Slice::new(&buffer[..])
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
    let term = str::from_utf8(&buf[..term_length])?;
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
    fn with_keyspace(keyspace: &Keyspace) -> Result<Self, Error> {
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
            Err(e) => Err(e.into()),
        })
    }

    pub fn put(
        &self,
        term: &str,
        document: DocumentId,
        data: &DocumentTermData,
    ) -> Result<(), Error> {
        let key = make_posting_list_key(term, document);
        Ok(self.db.insert(&key[..], data)?)
    }
}

impl TryFrom<Slice> for DocumentTermData {
    type Error = Error;

    fn try_from(value: Slice) -> Result<Self, Error> {
        let mut buffer = Cursor::new(&value[..]);
        let mut data = DocumentTermData::zero();
        data.deserialize(buffer.get_mut())?;
        Ok(data)
    }
}

impl From<&DocumentTermData> for Slice {
    fn from(value: &DocumentTermData) -> Self {
        let mut buffer = Vec::new();
        value.serialize(&mut buffer).unwrap();
        Slice::new(&buffer[..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::Config;

    #[test]
    fn test_term_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_term_store")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)?.delete(&keyspace)?;

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
        let keyspace = Config::new("/tmp/tangerine/test_document_store")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)?.delete(&keyspace)?;

        let store = DocumentStore::with_keyspace(&keyspace)?;

        let doc_data = DocumentData { length: 3 };
        store.put(123, &doc_data)?;

        let actual = store.get(123)?.unwrap();

        assert_eq!(3, actual.length);

        Ok(())
    }

    #[test]
    fn test_posting_list_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/test_posting_list_store")
            .open()
            .unwrap();

        IndexStore::new(&keyspace)?.delete(&keyspace)?;

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
