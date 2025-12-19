use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use super::test_utils::props_option;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                source::{
                    add_e::AddEAdapter, add_n::AddNAdapter, e_from_index::EFromIndexAdapter,
                    n_from_id::NFromIdAdapter,
                },
                util::{drop::Drop, update::UpdateAdapter},
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    props,
    protocol::value::Value,
};

fn setup_indexed_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let mut config = crate::helix_engine::traversal_core::config::Config::default();
    config.graph_config.as_mut().unwrap().edge_secondary_indices =
        Some(vec!["since".to_string()]);
    let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
    (temp_dir, Arc::new(storage))
}

fn to_result_iter(
    values: Vec<TraversalValue>,
) -> impl Iterator<Item = Result<TraversalValue, GraphError>> {
    values.into_iter().map(Ok)
}

#[test]
fn test_e_from_index_returns_edges() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let from_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let to_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("company", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "WorksAt",
            props_option(&arena, props! { "since" => "2020" }),
            from_id,
            to_id,
            false,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let key = "2020".to_string();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge.id());
}

#[test]
fn test_update_edge_secondary_indices() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let from_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let to_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("company", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "WorksAt",
            props_option(&arena, props! { "since" => "2020" }),
            from_id,
            to_id,
            false,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let old_key = "2020".to_string();
    let traversal = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &old_key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    G::new_mut_from_iter(&storage, &mut txn, traversal.into_iter(), &arena)
        .update(&[("since", Value::from("2021"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let new_key = "2021".to_string();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &new_key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edges.len(), 1);

    let edges = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &old_key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(edges.is_empty());
}

#[test]
fn test_drop_edge_removes_index_entries() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let from_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let to_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("company", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "WorksAt",
            props_option(&arena, props! { "since" => "2020" }),
            from_id,
            to_id,
            false,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let key = "2020".to_string();
    let traversal = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(edges.is_empty());
}

#[test]
fn test_drop_node_cleans_incident_edge_indices() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let from_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let to_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("company", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "WorksAt",
            props_option(&arena, props! { "since" => "2020" }),
            from_id,
            to_id,
            false,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&from_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let key = "2020".to_string();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_index("WorksAt", "since", &key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(edges.is_empty());
}
