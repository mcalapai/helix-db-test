use crate::{
    helix_engine::{
        indexing::secondary_index_not_found,
        traversal_core::{
            LMDB_STRING_HEADER_LENGTH, traversal_iter::RoTraversalIterator,
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    protocol::value::Value,
    utils::items::Node,
};
use serde::Serialize;

pub trait NFromIndexAdapter<'db, 'arena, 'txn, 's, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns a new iterator that will return the node from the secondary index.
    ///
    /// # Arguments
    ///
    /// * `index` - The name of the secondary index.
    /// * `key` - The key to search for in the secondary index.
    ///
    /// Note that both the `index` and `key` must be provided.
    /// The index must be a valid and existing secondary index and the key should match the type of the index.
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &'s K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone;

    /// Returns a new iterator that will return all nodes matching any of the
    /// provided keys from the secondary index.
    ///
    /// This is the index-backed equivalent of:
    /// `N<Type>::WHERE(_::{field}::IS_IN(keys))`.
    ///
    /// Duplicates in `keys` are tolerated; results are de-duplicated by node ID.
    fn n_from_index_in(
        self,
        label: &'s str,
        index: &'s str,
        keys: &'s [K],
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone;
}

impl<
    'db,
    'arena,
    'txn,
    's,
    K: Into<Value> + Serialize,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
> NFromIndexAdapter<'db, 'arena, 'txn, 's, K> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone,
    {
        let db = self
            .storage
            .secondary_indices
            .get(index)
            .ok_or_else(|| secondary_index_not_found(index))
            .unwrap();
        let label_as_bytes = label.as_bytes();
        let res = db
            .prefix_iter(self.txn, &bincode::serialize(&Value::from(key)).unwrap())
            .unwrap()
            .filter_map(move |item| {
                if let Ok((_, node_id)) = item &&
                 let Some(value) = self.storage.nodes_db.get(self.txn, &node_id).ok()? {
                    assert!(
                        value.len() >= LMDB_STRING_HEADER_LENGTH,
                        "value length does not contain header which means the `label` field was missing from the node on insertion"
                    );
                    let length_of_label_in_lmdb =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;

                    if length_of_label_in_lmdb != label.len() {
                        return None;
                    }

                    assert!(
                        value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                        "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                    );
                    let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                        ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                    if label_in_lmdb == label_as_bytes {
                        match Node::<'arena>::from_bincode_bytes(node_id, value, self.arena) {
                            Ok(node) => {
                                return Some(Ok(TraversalValue::Node(node)));
                            }
                            Err(e) => {
                                println!("{} Error decoding node: {:?}", line!(), e);
                                return Some(Err(GraphError::ConversionError(e.to_string())));
                            }
                        }
                    } else {
                        return None;
                    }
                }
                None
            });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: res,
        }
    }

    #[inline]
    fn n_from_index_in(
        self,
        label: &'s str,
        index: &'s str,
        keys: &'s [K],
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone,
    {
        let db = self
            .storage
            .secondary_indices
            .get(index)
            .ok_or_else(|| secondary_index_not_found(index))
            .unwrap();

        let label_as_bytes = label.as_bytes();
        let label_len = label.len();

        let mut seen = std::collections::HashSet::new();
        let mut out: Vec<Result<TraversalValue<'arena>, GraphError>> = Vec::new();

        for key in keys {
            let prefix = bincode::serialize(&Value::from(key)).unwrap();
            let iter = db.prefix_iter(self.txn, &prefix).unwrap();

            for item in iter {
                let (.., node_id) = match item {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };

                if !seen.insert(node_id) {
                    continue;
                }

                let value = match self.storage.nodes_db.get(self.txn, &node_id) {
                    Ok(Some(value)) => value,
                    _ => continue,
                };

                assert!(
                    value.len() >= LMDB_STRING_HEADER_LENGTH,
                    "value length does not contain header which means the `label` field was missing from the node on insertion"
                );

                let length_of_label_in_lmdb =
                    u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap())
                        as usize;

                if length_of_label_in_lmdb != label_len {
                    continue;
                }

                assert!(
                    value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                    "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                );

                let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                    ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                if label_in_lmdb != label_as_bytes {
                    continue;
                }

                match Node::<'arena>::from_bincode_bytes(node_id, value, self.arena) {
                    Ok(node) => out.push(Ok(TraversalValue::Node(node))),
                    Err(e) => out.push(Err(GraphError::ConversionError(e.to_string()))),
                }
            }
        }

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: out.into_iter(),
        }
    }
}
