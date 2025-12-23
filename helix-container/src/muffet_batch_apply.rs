use std::collections::HashMap;
use std::sync::Arc;

use bumpalo::Bump;
use helix_db::helix_engine::storage_core::HelixGraphStorage;
use helix_db::helix_engine::storage_core::storage_methods::StorageMethods;
use helix_db::helix_engine::traversal_core::LMDB_STRING_HEADER_LENGTH;
use helix_db::helix_engine::traversal_core::ops::g::G;
use helix_db::helix_engine::traversal_core::ops::in_::in_e::InEdgesAdapter;
use helix_db::helix_engine::traversal_core::ops::out::out_e::OutEdgesAdapter;
use helix_db::helix_engine::traversal_core::ops::source::add_e::AddEAdapter;
use helix_db::helix_engine::traversal_core::ops::source::add_n::AddNAdapter;
use helix_db::helix_engine::traversal_core::ops::source::e_from_index::EFromIndexAdapter;
use helix_db::helix_engine::traversal_core::ops::source::n_from_index::NFromIndexAdapter;
use helix_db::helix_engine::traversal_core::ops::util::drop::Drop;
use helix_db::helix_engine::traversal_core::ops::util::update::UpdateAdapter;
use helix_db::helix_engine::traversal_core::traversal_value::TraversalValue;
use helix_db::helix_engine::types::GraphError;
use helix_db::helix_gateway::router::router::HandlerInput;
use helix_db::protocol::response::Response;
use helix_db::protocol::value::Value;
use helix_db::utils::properties::ImmutablePropertiesMap;
use helix_macros::handler;
use sonic_rs::{Deserialize, json};

const SYNTHETIC_FQN_PREFIX: &str = "synthetic://";
const FILE_INDICES: &[&str] = &["stable_id", "path"];
const SYMBOL_INDICES: &[&str] = &["stable_id", "fqn"];
const SITE_INDICES: &[&str] = &["stable_id"];
const INGEST_STATE_INDICES: &[&str] = &["partition"];

#[derive(Debug, Deserialize)]
struct ApplyMutationBatchInput {
    pub partition: u32,
    pub offset: u64,
    pub to_rev: String,

    #[serde(default)]
    pub file_upsert: Option<FileUpsert>,
    #[serde(default)]
    pub symbol_upserts: Vec<SymbolUpsert>,
    #[serde(default)]
    pub symbol_drops: Vec<SymbolDrop>,
    #[serde(default)]
    pub edge_upserts: Vec<EdgeUpsert>,
    #[serde(default)]
    pub edge_drops: Vec<EdgeDrop>,
    #[serde(default)]
    pub site_upserts: Vec<SiteUpsert>,
    #[serde(default)]
    pub site_drops: Vec<SiteDrop>,
}

#[derive(Debug, Deserialize)]
struct FileUpsert {
    pub stable_id: String,
    pub path: String,
    pub hash: String,
    pub lang: String,
    pub namespace: String,
    pub repo: String,
}

#[derive(Debug, Deserialize)]
struct SymbolUpsert {
    pub stable_id: String,
    pub fqn: String,
    pub kind: String,
    pub file_stable_id: String,
    pub span_start: u32,
    pub span_end: u32,
    pub range_start_line: u32,
    pub range_start_character: u32,
    pub range_end_line: u32,
    pub range_end_character: u32,
    pub byte_start: u64,
    pub byte_end: u64,
    pub identifier_tokens: String,
    pub name: String,
    pub signature: Option<String>,
    pub doc: Option<String>,
    pub file_path: String,
    pub language: String,
    pub visibility: String,
    pub namespace: String,
    pub repo: String,
    pub default_x: f64,
    pub default_y: f64,
    pub default_z: f64,
}

#[derive(Debug, Deserialize)]
struct SymbolDrop {
    pub stable_id: String,
}

#[derive(Debug, Deserialize)]
struct EdgeUpsert {
    pub kind: String,
    pub stable_id: String,
    pub from_stable_id: String,
    pub to_stable_id: String,
    pub payload: Option<String>,
    pub namespace: String,
    pub repo: String,
}

#[derive(Debug, Deserialize)]
struct EdgeDrop {
    pub kind: String,
    pub stable_id: String,
}

#[derive(Debug, Deserialize)]
struct SiteUpsert {
    pub stable_id: String,
    pub kind: String,
    pub source_stable_id: String,
    pub container_stable_id: String,
    pub file_stable_id: String,
    pub file_path: String,
    pub uri: String,
    pub range_start_line: u32,
    pub range_start_character: u32,
    pub range_end_line: u32,
    pub range_end_character: u32,
    pub byte_start: u64,
    pub byte_end: u64,
    pub snippet: Option<String>,
    pub snippet_highlight_start_line: Option<u32>,
    pub snippet_highlight_start_character: Option<u32>,
    pub snippet_highlight_end_line: Option<u32>,
    pub snippet_highlight_end_character: Option<u32>,
    pub language: String,
    pub is_in_test: bool,
    pub is_conditional: bool,
    pub is_generated: bool,
    pub namespace: String,
    pub repo: String,
}

#[derive(Debug, Deserialize)]
struct SiteDrop {
    pub stable_id: String,
}

#[handler(is_write)]
#[allow(non_snake_case)]
pub fn ApplyMutationBatch(input: HandlerInput) -> Result<Response, GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let data = input
        .request
        .in_fmt
        .deserialize::<ApplyMutationBatchInput>(&input.request.body)?;

    let arena = Bump::new();
    let mut txn = db
        .graph_env
        .write_txn()
        .map_err(|e| GraphError::New(format!("Failed to start write transaction: {e:?}")))?;

    // In-batch stable_id -> node_id memoization to avoid repeated index lookups.
    let mut node_id_cache: HashMap<String, u128> = HashMap::new();

    if let Some(file) = &data.file_upsert {
        upsert_file(db.as_ref(), &mut txn, &arena, &mut node_id_cache, file)?;
    }

    drop_symbols(
        db.as_ref(),
        &mut txn,
        &arena,
        &mut node_id_cache,
        &data.symbol_drops,
    )?;
    upsert_symbols(
        db.as_ref(),
        &mut txn,
        &arena,
        &mut node_id_cache,
        &data.symbol_upserts,
    )?;

    drop_edges(db.as_ref(), &mut txn, &arena, &data.edge_drops)?;
    upsert_edges(
        db.as_ref(),
        &mut txn,
        &arena,
        &mut node_id_cache,
        &data.edge_upserts,
    )?;

    drop_site_edges(db.as_ref(), &mut txn, &arena, &data.site_drops)?;
    upsert_sites_and_edges(
        db.as_ref(),
        &mut txn,
        &arena,
        &mut node_id_cache,
        &data.site_upserts,
    )?;

    write_ingest_state(
        db.as_ref(),
        &mut txn,
        &arena,
        data.partition,
        data.offset,
        data.to_rev.as_str(),
    )?;

    txn.commit()
        .map_err(|e| GraphError::New(format!("Failed to commit transaction: {e:?}")))?;

    Ok(input.request.out_fmt.create_response(&json!({
        "ok": true,
        "partition": data.partition,
        "offset": data.offset,
        "to_rev": data.to_rev,
    })))
}

fn upsert_file<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    file: &FileUpsert,
) -> Result<(), GraphError> {
    let existing = G::new(db, txn, arena)
        .n_from_index("File", "stable_id", &file.stable_id)
        .collect::<Result<Vec<_>, _>>()?;

    if existing.is_empty() {
        let props = ImmutablePropertiesMap::new(
            6,
            [
                ("stable_id", Value::from(file.stable_id.as_str())),
                ("path", Value::from(file.path.as_str())),
                ("hash", Value::from(file.hash.as_str())),
                ("lang", Value::from(file.lang.as_str())),
                ("namespace", Value::from(file.namespace.as_str())),
                ("repo", Value::from(file.repo.as_str())),
            ]
            .into_iter(),
            arena,
        );
        let created = G::new_mut(db, arena, txn)
            .add_n("File", Some(props), Some(FILE_INDICES))
            .collect_to_obj()?;
        if let TraversalValue::Node(node) = created {
            node_id_cache.insert(file.stable_id.clone(), node.id);
        }
        return Ok(());
    }

    node_id_cache.insert(file.stable_id.clone(), first_node_id(&existing)?);

    let updated = G::new_mut_from_iter(db, txn, existing.into_iter(), arena)
        .update(&[
            ("path", Value::from(file.path.as_str())),
            ("hash", Value::from(file.hash.as_str())),
            ("lang", Value::from(file.lang.as_str())),
            ("namespace", Value::from(file.namespace.as_str())),
            ("repo", Value::from(file.repo.as_str())),
        ])
        .collect::<Result<Vec<_>, _>>()?;

    if updated.is_empty() {
        return Err(GraphError::New(
            "File update produced no results".to_string(),
        ));
    }

    Ok(())
}

fn drop_symbols<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    drops: &[SymbolDrop],
) -> Result<(), GraphError> {
    for drop in drops {
        node_id_cache.remove(&drop.stable_id);

        let nodes = G::new(db, txn, arena)
            .n_from_index("Symbol", "stable_id", &drop.stable_id)
            .collect::<Result<Vec<_>, _>>()?;
        if nodes.is_empty() {
            continue;
        }
        Drop::drop_traversal(nodes.into_iter().map(Ok), db, txn)?;
    }

    Ok(())
}

fn upsert_symbols<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    symbols: &[SymbolUpsert],
) -> Result<(), GraphError> {
    for symbol in symbols {
        if symbol.fqn.starts_with(SYNTHETIC_FQN_PREFIX) {
            upsert_placeholder_symbol(db, txn, arena, node_id_cache, symbol)?;
        } else {
            upsert_symbol_update_first(db, txn, arena, node_id_cache, symbol)?;
        }
    }

    Ok(())
}

fn upsert_symbol_update_first<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    symbol: &SymbolUpsert,
) -> Result<(), GraphError> {
    let existing = G::new(db, txn, arena)
        .n_from_index("Symbol", "stable_id", &symbol.stable_id)
        .collect::<Result<Vec<_>, _>>()?;

    if existing.is_empty() {
        let props = ImmutablePropertiesMap::new(
            24,
            [
                ("stable_id", Value::from(symbol.stable_id.as_str())),
                ("fqn", Value::from(symbol.fqn.as_str())),
                ("kind", Value::from(symbol.kind.as_str())),
                (
                    "file_stable_id",
                    Value::from(symbol.file_stable_id.as_str()),
                ),
                ("span_start", Value::from(symbol.span_start)),
                ("span_end", Value::from(symbol.span_end)),
                ("range_start_line", Value::from(symbol.range_start_line)),
                (
                    "range_start_character",
                    Value::from(symbol.range_start_character),
                ),
                ("range_end_line", Value::from(symbol.range_end_line)),
                (
                    "range_end_character",
                    Value::from(symbol.range_end_character),
                ),
                ("byte_start", Value::from(symbol.byte_start)),
                ("byte_end", Value::from(symbol.byte_end)),
                (
                    "identifier_tokens",
                    Value::from(symbol.identifier_tokens.as_str()),
                ),
                ("name", Value::from(symbol.name.as_str())),
                (
                    "signature",
                    Value::from(symbol.signature.as_deref().unwrap_or_default()),
                ),
                (
                    "doc",
                    Value::from(symbol.doc.as_deref().unwrap_or_default()),
                ),
                ("file_path", Value::from(symbol.file_path.as_str())),
                ("language", Value::from(symbol.language.as_str())),
                ("visibility", Value::from(symbol.visibility.as_str())),
                ("namespace", Value::from(symbol.namespace.as_str())),
                ("repo", Value::from(symbol.repo.as_str())),
                ("default_x", Value::from(symbol.default_x)),
                ("default_y", Value::from(symbol.default_y)),
                ("default_z", Value::from(symbol.default_z)),
            ]
            .into_iter(),
            arena,
        );

        let created = G::new_mut(db, arena, txn)
            .add_n("Symbol", Some(props), Some(SYMBOL_INDICES))
            .collect_to_obj()?;
        if let TraversalValue::Node(node) = created {
            node_id_cache.insert(symbol.stable_id.clone(), node.id);
        }
        return Ok(());
    }

    node_id_cache.insert(symbol.stable_id.clone(), first_node_id(&existing)?);

    G::new_mut_from_iter(db, txn, existing.into_iter(), arena)
        .update(&[
            ("fqn", Value::from(symbol.fqn.as_str())),
            ("kind", Value::from(symbol.kind.as_str())),
            (
                "file_stable_id",
                Value::from(symbol.file_stable_id.as_str()),
            ),
            ("span_start", Value::from(symbol.span_start)),
            ("span_end", Value::from(symbol.span_end)),
            ("range_start_line", Value::from(symbol.range_start_line)),
            (
                "range_start_character",
                Value::from(symbol.range_start_character),
            ),
            ("range_end_line", Value::from(symbol.range_end_line)),
            (
                "range_end_character",
                Value::from(symbol.range_end_character),
            ),
            ("byte_start", Value::from(symbol.byte_start)),
            ("byte_end", Value::from(symbol.byte_end)),
            (
                "identifier_tokens",
                Value::from(symbol.identifier_tokens.as_str()),
            ),
            ("name", Value::from(symbol.name.as_str())),
            (
                "signature",
                Value::from(symbol.signature.as_deref().unwrap_or_default()),
            ),
            (
                "doc",
                Value::from(symbol.doc.as_deref().unwrap_or_default()),
            ),
            ("file_path", Value::from(symbol.file_path.as_str())),
            ("language", Value::from(symbol.language.as_str())),
            ("visibility", Value::from(symbol.visibility.as_str())),
            ("namespace", Value::from(symbol.namespace.as_str())),
            ("repo", Value::from(symbol.repo.as_str())),
            ("default_x", Value::from(symbol.default_x)),
            ("default_y", Value::from(symbol.default_y)),
            ("default_z", Value::from(symbol.default_z)),
        ])
        .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}

fn upsert_placeholder_symbol<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    symbol: &SymbolUpsert,
) -> Result<(), GraphError> {
    let existing = G::new(db, txn, arena)
        .n_from_index("Symbol", "stable_id", &symbol.stable_id)
        .collect::<Result<Vec<_>, _>>()?;

    if !existing.is_empty() {
        node_id_cache.insert(symbol.stable_id.clone(), first_node_id(&existing)?);
        return Ok(());
    }

    let props = ImmutablePropertiesMap::new(
        24,
        [
            ("stable_id", Value::from(symbol.stable_id.as_str())),
            ("fqn", Value::from(symbol.fqn.as_str())),
            ("kind", Value::from(symbol.kind.as_str())),
            (
                "file_stable_id",
                Value::from(symbol.file_stable_id.as_str()),
            ),
            ("span_start", Value::from(symbol.span_start)),
            ("span_end", Value::from(symbol.span_end)),
            ("range_start_line", Value::from(symbol.range_start_line)),
            (
                "range_start_character",
                Value::from(symbol.range_start_character),
            ),
            ("range_end_line", Value::from(symbol.range_end_line)),
            (
                "range_end_character",
                Value::from(symbol.range_end_character),
            ),
            ("byte_start", Value::from(symbol.byte_start)),
            ("byte_end", Value::from(symbol.byte_end)),
            (
                "identifier_tokens",
                Value::from(symbol.identifier_tokens.as_str()),
            ),
            ("name", Value::from(symbol.name.as_str())),
            (
                "signature",
                Value::from(symbol.signature.as_deref().unwrap_or_default()),
            ),
            (
                "doc",
                Value::from(symbol.doc.as_deref().unwrap_or_default()),
            ),
            ("file_path", Value::from(symbol.file_path.as_str())),
            ("language", Value::from(symbol.language.as_str())),
            ("visibility", Value::from(symbol.visibility.as_str())),
            ("namespace", Value::from(symbol.namespace.as_str())),
            ("repo", Value::from(symbol.repo.as_str())),
            ("default_x", Value::from(symbol.default_x)),
            ("default_y", Value::from(symbol.default_y)),
            ("default_z", Value::from(symbol.default_z)),
        ]
        .into_iter(),
        arena,
    );

    let created = G::new_mut(db, arena, txn)
        .add_n("Symbol", Some(props), Some(SYMBOL_INDICES))
        .collect_to_obj()?;
    if let TraversalValue::Node(node) = created {
        node_id_cache.insert(symbol.stable_id.clone(), node.id);
    }
    Ok(())
}

fn drop_edges<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    drops: &[EdgeDrop],
) -> Result<(), GraphError> {
    for drop in drops {
        let edges = G::new(db, txn, arena)
            .e_from_index(drop.kind.as_str(), "stable_id", &drop.stable_id)
            .collect::<Result<Vec<_>, _>>()?;
        if edges.is_empty() {
            continue;
        }
        Drop::drop_traversal(edges.into_iter().map(Ok), db, txn)?;
    }

    Ok(())
}

fn upsert_edges<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    edges: &[EdgeUpsert],
) -> Result<(), GraphError> {
    for edge in edges {
        let existing = G::new(db, txn, arena)
            .e_from_index(edge.kind.as_str(), "stable_id", &edge.stable_id)
            .collect::<Result<Vec<_>, _>>()?;

        if existing.is_empty() {
            let from_id =
                resolve_endpoint_node_id(db, txn, arena, node_id_cache, &edge.from_stable_id)?;
            let to_id =
                resolve_endpoint_node_id(db, txn, arena, node_id_cache, &edge.to_stable_id)?;

            let mut props_vec: Vec<(&'static str, Value)> = vec![
                ("stable_id", Value::from(edge.stable_id.as_str())),
                ("namespace", Value::from(edge.namespace.as_str())),
                ("repo", Value::from(edge.repo.as_str())),
            ];
            if let Some(payload) = edge.payload.as_deref() {
                props_vec.push(("payload", Value::from(payload)));
            }
            let props = ImmutablePropertiesMap::new(props_vec.len(), props_vec.into_iter(), arena);

            let _ = G::new_mut(db, arena, txn)
                .add_edge(edge.kind.as_str(), Some(props), from_id, to_id, false)
                .collect_to_obj()?;
            continue;
        }

        let mut props_vec: Vec<(&'static str, Value)> = vec![
            ("namespace", Value::from(edge.namespace.as_str())),
            ("repo", Value::from(edge.repo.as_str())),
        ];
        if let Some(payload) = edge.payload.as_deref() {
            props_vec.push(("payload", Value::from(payload)));
        }

        G::new_mut_from_iter(db, txn, existing.into_iter(), arena)
            .update(props_vec.as_slice())
            .collect::<Result<Vec<_>, _>>()?;
    }

    Ok(())
}

fn drop_site_edges<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    drops: &[SiteDrop],
) -> Result<(), GraphError> {
    for drop in drops {
        let sites = G::new(db, txn, arena)
            .n_from_index("Site", "stable_id", &drop.stable_id)
            .collect::<Result<Vec<_>, _>>()?;

        for site in sites {
            let site_node = match site {
                TraversalValue::Node(node) => node,
                _ => continue,
            };

            let in_edges = G::from_iter(
                db,
                txn,
                std::iter::once(TraversalValue::Node(site_node)),
                arena,
            )
            .in_e("SiteInSymbol")
            .collect::<Result<Vec<_>, _>>()?;
            if !in_edges.is_empty() {
                Drop::drop_traversal(in_edges.into_iter().map(Ok), db, txn)?;
            }

            let out_edges = G::from_iter(
                db,
                txn,
                std::iter::once(TraversalValue::Node(site_node)),
                arena,
            )
            .out_e("SiteUsesSymbol")
            .collect::<Result<Vec<_>, _>>()?;
            if !out_edges.is_empty() {
                Drop::drop_traversal(out_edges.into_iter().map(Ok), db, txn)?;
            }
        }
    }

    Ok(())
}

fn upsert_sites_and_edges<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    sites: &[SiteUpsert],
) -> Result<(), GraphError> {
    // Drop + re-add Site nodes (idempotent by stable_id), then attach site edges.
    for site in sites {
        // Drop existing Site node (and any edges) keyed by stable_id.
        let existing = G::new(db, txn, arena)
            .n_from_index("Site", "stable_id", &site.stable_id)
            .collect::<Result<Vec<_>, _>>()?;
        if !existing.is_empty() {
            Drop::drop_traversal(existing.into_iter().map(Ok), db, txn)?;
        }

        let snippet = site.snippet.as_deref().unwrap_or_default();
        let props = ImmutablePropertiesMap::new(
            24,
            [
                ("stable_id", Value::from(site.stable_id.as_str())),
                ("kind", Value::from(site.kind.as_str())),
                (
                    "source_stable_id",
                    Value::from(site.source_stable_id.as_str()),
                ),
                (
                    "container_stable_id",
                    Value::from(site.container_stable_id.as_str()),
                ),
                ("file_stable_id", Value::from(site.file_stable_id.as_str())),
                ("file_path", Value::from(site.file_path.as_str())),
                ("uri", Value::from(site.uri.as_str())),
                ("range_start_line", Value::from(site.range_start_line)),
                (
                    "range_start_character",
                    Value::from(site.range_start_character),
                ),
                ("range_end_line", Value::from(site.range_end_line)),
                ("range_end_character", Value::from(site.range_end_character)),
                ("byte_start", Value::from(site.byte_start)),
                ("byte_end", Value::from(site.byte_end)),
                ("snippet", Value::from(snippet)),
                (
                    "snippet_highlight_start_line",
                    Value::from(site.snippet_highlight_start_line.unwrap_or(0)),
                ),
                (
                    "snippet_highlight_start_character",
                    Value::from(site.snippet_highlight_start_character.unwrap_or(0)),
                ),
                (
                    "snippet_highlight_end_line",
                    Value::from(site.snippet_highlight_end_line.unwrap_or(0)),
                ),
                (
                    "snippet_highlight_end_character",
                    Value::from(site.snippet_highlight_end_character.unwrap_or(0)),
                ),
                ("language", Value::from(site.language.as_str())),
                (
                    "is_in_test",
                    Value::from(if site.is_in_test { 1u32 } else { 0u32 }),
                ),
                (
                    "is_conditional",
                    Value::from(if site.is_conditional { 1u32 } else { 0u32 }),
                ),
                (
                    "is_generated",
                    Value::from(if site.is_generated { 1u32 } else { 0u32 }),
                ),
                ("namespace", Value::from(site.namespace.as_str())),
                ("repo", Value::from(site.repo.as_str())),
            ]
            .into_iter(),
            arena,
        );

        let created = G::new_mut(db, arena, txn)
            .add_n("Site", Some(props), Some(SITE_INDICES))
            .collect_to_obj()?;
        let site_node_id = match created {
            TraversalValue::Node(node) => node.id,
            _ => continue,
        };
        node_id_cache.insert(site.stable_id.clone(), site_node_id);
    }

    // SiteInSymbol: Symbol -> Site (container)
    for site in sites {
        if site.container_stable_id.is_empty() {
            continue;
        }
        let Some(site_id) = node_id_cache.get(&site.stable_id).copied() else {
            continue;
        };
        let container_id =
            resolve_endpoint_node_id(db, txn, arena, node_id_cache, &site.container_stable_id)?;

        let props = ImmutablePropertiesMap::new(
            3,
            [
                ("stable_id", Value::from(site.stable_id.as_str())),
                ("namespace", Value::from(site.namespace.as_str())),
                ("repo", Value::from(site.repo.as_str())),
            ]
            .into_iter(),
            arena,
        );

        let _ = G::new_mut(db, arena, txn)
            .add_edge("SiteInSymbol", Some(props), container_id, site_id, false)
            .collect_to_obj()?;
    }

    // SiteUsesSymbol: Site -> Symbol (source)
    for site in sites {
        if site.source_stable_id.is_empty() {
            continue;
        }
        let Some(site_id) = node_id_cache.get(&site.stable_id).copied() else {
            continue;
        };
        let source_id =
            resolve_endpoint_node_id(db, txn, arena, node_id_cache, &site.source_stable_id)?;

        let props = ImmutablePropertiesMap::new(
            3,
            [
                ("stable_id", Value::from(site.stable_id.as_str())),
                ("namespace", Value::from(site.namespace.as_str())),
                ("repo", Value::from(site.repo.as_str())),
            ]
            .into_iter(),
            arena,
        );

        let _ = G::new_mut(db, arena, txn)
            .add_edge("SiteUsesSymbol", Some(props), site_id, source_id, false)
            .collect_to_obj()?;
    }

    Ok(())
}

fn write_ingest_state<'db>(
    db: &'db HelixGraphStorage,
    txn: &mut heed3::RwTxn<'db>,
    arena: &Bump,
    partition: u32,
    offset: u64,
    to_rev: &str,
) -> Result<(), GraphError> {
    let nodes = node_ids_for_index_u32(db, txn, "IngestState", "partition", partition)?;

    if nodes.is_empty() {
        let props = ImmutablePropertiesMap::new(
            3,
            [
                ("partition", Value::from(partition)),
                ("offset", Value::from(offset)),
                ("to_rev", Value::from(to_rev)),
            ]
            .into_iter(),
            arena,
        );
        let _ = G::new_mut(db, arena, txn)
            .add_n("IngestState", Some(props), Some(INGEST_STATE_INDICES))
            .collect_to_obj()?;
        return Ok(());
    }

    let mut items = Vec::with_capacity(nodes.len());
    for id in nodes {
        let node = db.get_node(txn, &id, arena)?;
        items.push(TraversalValue::Node(node));
    }

    G::new_mut_from_iter(db, txn, items.into_iter(), arena)
        .update(&[
            ("offset", Value::from(offset)),
            ("to_rev", Value::from(to_rev)),
        ])
        .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}

fn node_ids_for_index_u32<'db>(
    db: &'db HelixGraphStorage,
    txn: &heed3::RoTxn<'db>,
    label: &str,
    index: &str,
    key: u32,
) -> Result<Vec<u128>, GraphError> {
    let Some(index_db) = db.secondary_indices.get(index) else {
        return Err(GraphError::New(format!(
            "Secondary index not found: {index}"
        )));
    };

    let key_bytes = bincode::serialize(&Value::from(key))?;
    let label_bytes = label.as_bytes();
    let label_len = label.len();

    let mut out = Vec::new();
    let iter = index_db.prefix_iter(txn, &key_bytes)?;
    for item in iter {
        let (.., node_id) = item?;
        let Some(value) = db.nodes_db.get(txn, &node_id)? else {
            continue;
        };
        if !bytes_have_label(&value, label_bytes, label_len) {
            continue;
        }
        out.push(node_id);
    }

    Ok(out)
}

fn bytes_have_label(value: &[u8], label_bytes: &[u8], label_len: usize) -> bool {
    if value.len() < LMDB_STRING_HEADER_LENGTH {
        return false;
    }

    let length_of_label_in_lmdb =
        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;
    if length_of_label_in_lmdb != label_len {
        return false;
    }

    if value.len() < length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH {
        return false;
    }

    let label_in_lmdb =
        &value[LMDB_STRING_HEADER_LENGTH..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];
    label_in_lmdb == label_bytes
}

fn resolve_endpoint_node_id<'db>(
    db: &'db HelixGraphStorage,
    txn: &heed3::RoTxn<'db>,
    arena: &Bump,
    node_id_cache: &mut HashMap<String, u128>,
    stable_id: &String,
) -> Result<u128, GraphError> {
    if let Some(id) = node_id_cache.get(stable_id).copied() {
        return Ok(id);
    }

    let stable_id_str = stable_id.as_str();
    let label = if stable_id_str.starts_with("file:") {
        "File"
    } else if stable_id_str.starts_with("site:") {
        "Site"
    } else {
        "Symbol"
    };

    let nodes = G::new(db, txn, arena)
        .n_from_index(label, "stable_id", stable_id)
        .collect::<Result<Vec<_>, _>>()?;

    if nodes.is_empty() {
        return Err(GraphError::New(format!(
            "Missing node endpoint: stable_id={stable_id_str} (label={label})"
        )));
    }

    let id = first_node_id(&nodes)?;
    node_id_cache.insert(stable_id.clone(), id);
    Ok(id)
}

fn first_node_id(values: &[TraversalValue<'_>]) -> Result<u128, GraphError> {
    values
        .iter()
        .find_map(|value| match value {
            TraversalValue::Node(node) => Some(node.id),
            _ => None,
        })
        .ok_or_else(|| GraphError::New("Expected node traversal value".to_string()))
}
