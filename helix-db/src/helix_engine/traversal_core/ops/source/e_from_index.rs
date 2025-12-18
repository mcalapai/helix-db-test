use crate::{
    helix_engine::{
        indexing::edge_secondary_index_not_found,
        traversal_core::{
            traversal_iter::RoTraversalIterator, traversal_value::TraversalValue,
            LMDB_STRING_HEADER_LENGTH,
        },
        types::GraphError,
    },
    protocol::value::Value,
    utils::items::Edge,
};
use serde::Serialize;

pub trait EFromIndexAdapter<'db, 'arena, 'txn, 's, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns a new iterator that will return the edge from the secondary index.
    ///
    /// # Arguments
    ///
    /// * `index` - The name of the secondary index.
    /// * `key` - The key to search for in the secondary index.
    ///
    /// Note that both the `index` and `key` must be provided.
    /// The index must be a valid and existing secondary index and the key should match the type of the index.
    fn e_from_index(
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
}

impl<
    'db,
    'arena,
    'txn,
    's,
    K: Into<Value> + Serialize,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
> EFromIndexAdapter<'db, 'arena, 'txn, 's, K> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn e_from_index(
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
        K: Into<Value> + Serialize + Clone,
    {
        let db = self
            .storage
            .edge_secondary_indices
            .get(index)
            .ok_or_else(|| edge_secondary_index_not_found(index))
            .unwrap();
        let label_as_bytes = label.as_bytes();
        let res = db
            .prefix_iter(self.txn, &bincode::serialize(&Value::from(key)).unwrap())
            .unwrap()
            .filter_map(move |item| {
                if let Ok((_, edge_id)) = item
                    && let Some(value) = self.storage.edges_db.get(self.txn, &edge_id).ok()?
                {
                    assert!(
                        value.len() >= LMDB_STRING_HEADER_LENGTH,
                        "value length does not contain header which means the `label` field was missing from the edge on insertion"
                    );
                    let length_of_label_in_lmdb =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap())
                            as usize;

                    if length_of_label_in_lmdb != label.len() {
                        return None;
                    }

                    assert!(
                        value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                        "value length is not at least the header length plus the label length meaning there has been a corruption on edge insertion"
                    );
                    let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                        ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                    if label_in_lmdb == label_as_bytes {
                        match Edge::<'arena>::from_bincode_bytes(edge_id, value, self.arena) {
                            Ok(edge) => {
                                return Some(Ok(TraversalValue::Edge(edge)));
                            }
                            Err(e) => {
                                println!("{} Error decoding edge: {:?}", line!(), e);
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
}
