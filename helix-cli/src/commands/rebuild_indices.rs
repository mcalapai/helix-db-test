use crate::docker::DockerManager;
use crate::project::ProjectContext;
use crate::prompts;
use crate::utils::helixc_utils::{
    analyze_source, collect_hx_files, generate_content, parse_content,
};
use crate::utils::{print_confirm, print_status, print_success, print_warning};
use eyre::Result;
use helix_db::helix_engine::storage_core::HelixGraphStorage;
use helix_db::helix_engine::storage_core::version_info::VersionInfo;
use helix_db::helix_engine::traversal_core::config::Config;

pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;

    let instance_name = match instance {
        Some(name) => name,
        None => {
            let instances = project.config.list_instances_with_types();
            prompts::select_instance(&instances)?
        }
    };

    let instance_config = project.config.get_instance(&instance_name)?;
    if !instance_config.is_local() {
        return Err(eyre::eyre!(
            "Index rebuild is only supported for local instances."
        ));
    }

    let docker = DockerManager::new(&project);
    let mut instance_running = false;
    if let Ok(statuses) = docker.get_project_status() {
        if let Some(status) = statuses
            .iter()
            .find(|status| status.instance_name == instance_name)
        {
            instance_running = status.status.starts_with("Up");
        }
    } else {
        print_warning(
            "Unable to determine if the instance is running. Ensure it is stopped before rebuild.",
        );
    }

    if instance_running {
        print_warning(
            "Instance appears to be running. Rebuild requires the instance to be stopped.",
        );
        let confirmed = print_confirm(&format!("Stop instance '{instance_name}' now?"))?;
        if !confirmed {
            print_status("REBUILD", "Cancelled by user");
            return Ok(());
        }
        docker.stop_instance(&instance_name)?;
    }

    let env_path = project.instance_volume(&instance_name).join("user");
    if !env_path.exists() {
        return Err(eyre::eyre!(
            "Instance LMDB environment not found at {:?}",
            env_path
        ));
    }

    print_status("ANALYZE", "Analyzing schema for indexed fields...");
    let hx_files = collect_hx_files(&project.root, &project.config.project.queries)?;
    let content = generate_content(&hx_files)?;
    let source = parse_content(&content)?;
    let generated = analyze_source(source, &content.files)?;

    let mut config: Config = serde_json::from_value(instance_config.to_legacy_json())?;
    let graph_config = config.graph_config.get_or_insert_with(Default::default);
    graph_config.secondary_indices = Some(generated.secondary_indices.clone());
    graph_config.edge_secondary_indices = Some(generated.edge_secondary_indices.clone());

    print_status(
        "REBUILD",
        &format!(
            "Rebuilding node indices [{}] and edge indices [{}]",
            if generated.secondary_indices.is_empty() {
                "none".to_string()
            } else {
                generated.secondary_indices.join(", ")
            },
            if generated.edge_secondary_indices.is_empty() {
                "none".to_string()
            } else {
                generated.edge_secondary_indices.join(", ")
            }
        ),
    );

    let env_path_str = env_path
        .to_str()
        .ok_or_else(|| eyre::eyre!("Invalid path: {:?}", env_path))?;

    let storage = HelixGraphStorage::new(env_path_str, config, VersionInfo::default())?;
    let mut txn = storage.graph_env.write_txn()?;
    storage.rebuild_indices(&mut txn)?;
    txn.commit()?;

    print_success("Secondary indices rebuilt successfully");
    Ok(())
}
