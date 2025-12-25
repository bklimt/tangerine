use crate::error::Error;
use crate::index::TermData;
use bytes::{Buf, BufMut, BytesMut};
use fjall::{Partition, Slice};

pub struct TermStore {
    db: Partition,
}

impl TermStore {
    pub fn get(&self, term: &str) -> Result<Option<TermData>, Error> {
        match self.db.get(term) {
            Ok(Some(slice)) => TermData::try_from(slice).map(|d| Some(d)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::FjallError(e)),
        }
    }

    pub fn put(&mut self, term: &str, data: &TermData) -> Result<(), Error> {
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

#[cfg(test)]
mod tests {
    use fjall::{Config, PartitionCreateOptions};

    use super::*;

    #[test]
    fn test_term_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/testdata").open()?;
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("basic", options)?;
        let mut store = TermStore { db };

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
}
