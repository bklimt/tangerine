use fjall::{Error, Partition, Slice};

pub trait KeyValueStore {
    // TODO: Clean up these types.
    fn prefix(&self, prefix: &str) -> impl Iterator<Item = Result<(Slice, Slice), Error>>;
}

struct FjallStore {
    db: Partition,
}

impl KeyValueStore for FjallStore {
    fn prefix(&self, prefix: &str) -> impl Iterator<Item = Result<(Slice, Slice), Error>> {
        self.db.prefix(prefix)
    }
}

#[cfg(test)]
mod tests {
    use fjall::{Config, PartitionCreateOptions};

    use super::*;

    #[test]
    fn test_fjall_store() -> Result<(), Error> {
        let keyspace = Config::new("/tmp/tangerine/testdata").open()?;
        let options = PartitionCreateOptions::default();
        let db = keyspace.open_partition("basic", options)?;
        let store = FjallStore { db };
        store.db.insert("a", "b")?;

        let actual: Result<Vec<(Slice, Slice)>, Error> = store.prefix("a").collect();
        let actual = actual?;

        assert_eq!(1, actual.len());
        assert_eq!(
            (Slice::new("a".as_bytes()), Slice::new("b".as_bytes())),
            *actual.get(0).unwrap()
        );

        Ok(())
    }
}
