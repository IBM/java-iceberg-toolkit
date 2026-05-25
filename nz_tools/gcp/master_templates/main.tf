# ==============================================================================
# GCP Netezza BYOC Infrastructure - Root Module
# ==============================================================================
# Dependency chain: VPC → Artifact Registry → GKE → Cloud Functions → Pub/Sub → Workflows
# ==============================================================================

# ==============================================================================
# LOCAL VARIABLES - Common Labels and Naming
# ==============================================================================

locals {
  # Naming prefix for all resources: ibm-byoc-${dataplane_id}
  naming_prefix = "ibm-byoc-${var.dataplane_id}"

  # Common labels for all resources
  common_labels = {
    ibm_byoc              = var.dataplane_id
    ibmbyoc_dataplane_crn = replace(var.dataplane_crn, ":", "_")
    ibmbyoc_id            = var.dataplane_id
    ibmbyoc_product       = var.byoc_engine_label
    ibmbyoc_region        = var.region
  }
}

# ==============================================================================
# MODULE: VPC (Created First)
# ==============================================================================

module "vpc" {
  source = "./modules/vpc"

  project_id        = var.project_id
  region            = var.region
  naming_prefix     = local.naming_prefix
  dataplane_id      = var.dataplane_id
  dataplane_crn     = var.dataplane_crn
  byoc_engine_label = var.byoc_engine_label
  routing_mode      = var.vpc_routing_mode
  mtu               = var.vpc_mtu

  subnets = var.vpc_subnets

  create_nat_gateway            = var.create_nat_gateway
  router_asn                    = var.router_asn
  nat_log_config                = var.nat_log_config
  create_default_firewall_rules = var.create_default_firewall_rules

  create_vpc_connector        = var.create_vpc_connector
  vpc_connector_name          = var.vpc_connector_name
  vpc_connector_ip_cidr_range = var.vpc_connector_ip_cidr_range
  vpc_connector_min_instances = var.vpc_connector_min_instances
  vpc_connector_max_instances = var.vpc_connector_max_instances
  vpc_connector_machine_type  = var.vpc_connector_machine_type

  labels = local.common_labels
}
# ==============================================================================
# MODULE: ARTIFACT REGISTRY (Created after VPC, before Cloud Functions)
# ==============================================================================

module "artifact_registry" {
  source = "./modules/artifact_registry"

  project_id        = var.project_id
  region            = var.region
  naming_prefix     = local.naming_prefix
  dataplane_id      = var.dataplane_id
  dataplane_crn     = var.dataplane_crn
  byoc_engine_label = var.byoc_engine_label
  ops_sa_email      = var.ops_service_account

  # Optional configurations
  enable_immutable_tags         = var.artifact_registry_enable_immutable_tags
  untagged_image_retention_days = var.artifact_registry_untagged_retention_days
  keep_recent_versions          = var.artifact_registry_keep_versions
  enable_vulnerability_scanning = var.artifact_registry_enable_scanning
  gke_sa_email                  = var.gke_sa_email

  labels = local.common_labels
}

# ==============================================================================
# MODULE: GKE
# ==============================================================================

module "gke" {
  source = "./modules/gke"

  project_id        = var.project_id
  region            = var.region
  naming_prefix     = local.naming_prefix
  dataplane_id      = var.dataplane_id
  dataplane_crn     = var.dataplane_crn
  byoc_engine_label = var.byoc_engine_label

  location    = var.gke_location
  description = var.gke_cluster_description

  network    = module.vpc.vpc_self_link
  subnetwork = module.vpc.subnets[var.gke_subnet_key].self_link

  pods_secondary_range_name     = var.gke_pods_secondary_range_name
  services_secondary_range_name = var.gke_services_secondary_range_name

  deletion_protection = var.gke_deletion_protection

  release_channel            = var.gke_release_channel
  node_locations             = var.gke_node_locations
  datapath_provider          = var.gke_datapath_provider
  enable_shielded_nodes      = var.gke_enable_shielded_nodes
  enable_private_nodes       = var.gke_enable_private_nodes
  enable_private_endpoint    = var.gke_enable_private_endpoint
  master_ipv4_cidr_block     = var.gke_master_ipv4_cidr_block
  master_authorized_networks = var.gke_master_authorized_networks

  # Database encryption with customer-managed keys
  database_encryption_key_name = var.gke_database_encryption_key_name

  logging_components        = var.gke_logging_components
  monitoring_components     = var.gke_monitoring_components
  enable_managed_prometheus = var.gke_enable_managed_prometheus

  enable_http_load_balancing            = var.gke_enable_http_load_balancing
  enable_gce_persistent_disk_csi_driver = var.gke_enable_gce_persistent_disk_csi_driver
  enable_gcp_filestore_csi_driver       = var.gke_enable_gcp_filestore_csi_driver
  enable_dns_cache                      = var.gke_enable_dns_cache
  enable_gke_backup                     = var.gke_enable_backup

  # Maintenance Policy
  enable_maintenance_policy           = var.gke_enable_maintenance_policy
  maintenance_exclusions              = var.gke_maintenance_exclusions
  maintenance_exclusion_duration_days = var.gke_maintenance_exclusion_duration_days
  use_dynamic_maintenance_window      = var.gke_use_dynamic_maintenance_window
  daily_maintenance_window            = var.gke_daily_maintenance_window
  recurring_maintenance_window        = var.gke_recurring_maintenance_window

  # Node Auto-Provisioning
  enable_node_auto_provisioning    = var.gke_enable_node_auto_provisioning
  node_auto_provisioning_locations = var.gke_node_auto_provisioning_locations
  node_autoscaling_profile         = var.gke_node_autoscaling_profile
  node_auto_provisioning_limits    = var.gke_node_auto_provisioning_limits
  node_auto_provisioning_defaults  = var.gke_node_auto_provisioning_defaults

  default_node_pool = var.gke_default_node_pool

  labels = local.common_labels
}

# ==============================================================================
# MODULE: PUB/SUB (Created before Cloud Functions for Eventarc trigger)
# ==============================================================================

module "pubsub" {
  source = "./modules/pubsub"

  project_id        = var.project_id
  region            = var.region
  naming_prefix     = local.naming_prefix
  dataplane_id      = var.dataplane_id
  dataplane_crn     = var.dataplane_crn
  byoc_engine_label = var.byoc_engine_label

  # Service accounts
  send_dataplane_status_sa      = var.send_dataplane_status_sa

  # Pub/Sub configuration
  ack_deadline_seconds  = var.input_queue_ack_deadline_seconds
  max_delivery_attempts = var.max_delivery_attempts

  labels = local.common_labels
}

# ==============================================================================
# MODULE: CLOUD FUNCTIONS (Created after Pub/Sub for Eventarc trigger)
# ==============================================================================

module "cloud_functions" {
  source = "./modules/cloud_functions"

  # VPC connector reference creates implicit dependency on VPC
  # Pub/Sub topic reference creates implicit dependency on Pub/Sub

  project_id             = var.project_id
  region                 = var.region
  naming_prefix          = local.naming_prefix
  function_source_bucket = var.function_source_bucket
  ops_sa_email           = var.ops_service_account # Operations SA for function execution

  # Runtime configuration
  python_runtime     = var.python_runtime
  memory_mb          = var.function_memory_mb
  timeout_seconds    = var.function_timeout_seconds
  max_instance_count = var.function_max_instances
  min_instance_count = var.function_min_instances

  # Source code paths
  send_dataplane_status_source_object = var.send_dataplane_status_source_object
  kube_api_proxy_source_object        = var.kube_api_proxy_source_object
  install_operator_source_object      = var.install_operator_source_object
  get_operator_status_source_object   = var.get_operator_status_source_object
  common_serverless_source_object     = var.common_serverless_source_object

  # Pub/Sub SA and Topic for Eventarc trigger
  pubsub_invoker_service_account = var.pubsub_invoker_service_account
  input_queue_topic_id           = module.pubsub.input_queue_topic_id

  # Environment variables (including Artifact Registry)
  common_env_vars = merge(var.function_common_env_vars, {
    ARTIFACT_REGISTRY_URL  = module.artifact_registry.repository_url
    ARTIFACT_REGISTRY_PATH = module.artifact_registry.full_repository_path
    CONTAINER_REGISTRY     = module.artifact_registry.registry_endpoint
  })
  send_dataplane_status_env_vars = var.send_dataplane_status_env_vars
  kube_api_proxy_env_vars        = var.kube_api_proxy_env_vars
  install_operator_env_vars      = var.install_operator_env_vars
  get_operator_status_env_vars   = var.get_operator_status_env_vars
  common_serverless_env_vars     = var.common_serverless_env_vars

  # VPC Configuration - Use VPC connector from VPC module
  vpc_connector_egress_settings = var.vpc_connector_egress_settings
  vpc_connector                 = module.vpc.vpc_connector_id

  labels = local.common_labels
}

# ==============================================================================
# MODULE: WORKFLOWS
# ==============================================================================

module "workflows" {
  source = "./modules/workflows"

  # Removed depends_on to allow data sources to be evaluated during plan phase
  # Dependencies are handled naturally through resource references

  project_id        = var.project_id
  region            = var.region
  naming_prefix     = local.naming_prefix
  dataplane_id      = var.dataplane_id
  dataplane_crn     = var.dataplane_crn
  byoc_engine_label = var.byoc_engine_label

  # Workflow source files from GCS
  workflow_definitions_bucket         = var.workflow_source_bucket
  cluster_setup_workflow_file         = var.cluster_setup_workflow_object
  delete_cluster_setup_workflow_file  = var.delete_cluster_setup_workflow_object
  netezza_engine_deploy_workflow_file = var.engine_deploy_workflow_object

  # Service account for workflow execution
  ops_sa_email = var.ops_service_account # For runtime operations
  kms_key_name = var.workflows_kms_key

  labels = local.common_labels
}

