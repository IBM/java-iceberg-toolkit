# ==============================================================================
# Root Module Variables - GCP Netezza BYOC Infrastructure
# ==============================================================================

# ==============================================================================
# REQUIRED VARIABLES
# ==============================================================================

variable "project_id" {
  description = "GCP project ID where resources will be deployed"
  type        = string
}

variable "region" {
  description = "GCP region for resource deployment"
  type        = string
}

# ------------------------------------------------------------------------------
# SERVICE ACCOUNT
# ------------------------------------------------------------------------------
# Operations service account used by Cloud Functions and Workflows for runtime
# execution and resource management.
# ------------------------------------------------------------------------------

variable "ops_service_account" {
  description = "Operations service account email - Used by Cloud Functions and Workflows for runtime execution and resource management."
  type        = string
}

# ==============================================================================
# DATAPLANE IDENTIFICATION
# ==============================================================================

variable "dataplane_id" {
  description = "Unique identifier for the dataplane (e.g., dp-12345). Used to construct resource names with prefix 'ibm-byoc-{dataplane_id}'"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$", var.dataplane_id))
    error_message = "dataplane_id must be lowercase alphanumeric with hyphens, start and end with alphanumeric, max 63 chars (to fit GCP naming limits with 'ibm-byoc-' prefix)"
  }
}

variable "dataplane_crn" {
  description = "Cloud Resource Name (CRN) for the dataplane - Optional, used for reference only, not applied to GCP resources"
  type        = string
  default     = ""
}

# ==============================================================================
# PUB/SUB SERVICE ACCOUNTS
# ==============================================================================

variable "pubsub_invoker_service_account" {
  description = "Service account email that can invoke Cloud Functions via Pub/Sub"
  type        = string
}

variable "send_dataplane_status_sa" {
  description = "Service account email for send_dataplane_status function that publishes to status topic"
  type        = string
}

# ==============================================================================
# ARTIFACT REGISTRY CONFIGURATION
# ==============================================================================

# Artifact Registry naming is now handled automatically by the naming_prefix

variable "artifact_registry_enable_immutable_tags" {
  description = "Enable immutable tags in Artifact Registry"
  type        = bool
  default     = true
}

variable "artifact_registry_untagged_retention_days" {
  description = "Days to retain untagged images in Artifact Registry"
  type        = number
  default     = 30
}

variable "artifact_registry_keep_versions" {
  description = "Number of recent image versions to keep in Artifact Registry"
  type        = number
  default     = 10
}

variable "artifact_registry_enable_scanning" {
  description = "Enable vulnerability scanning for container images"
  type        = bool
  default     = true
}

variable "gke_sa_email" {
  description = "GKE service account email for pulling images from Artifact Registry (optional)"
  type        = string
  default     = null
}

variable "byoc_engine_label" {
  description = "BYOC engine label for resource labeling"
  type        = string
  default     = "netezza"
}


# ==============================================================================
# GKE CONFIGURATION
# ==============================================================================

variable "gke_location" {
  description = "GKE cluster location"
  type        = string
}

variable "gke_cluster_description" {
  description = "Description of the GKE cluster"
  type        = string
  default     = "IBM BYOC GKE cluster"
}

variable "gke_subnet_key" {
  description = "Key of the subnet in vpc_subnets map to use for GKE"
  type        = string
  default     = "primary"
}

variable "gke_pods_secondary_range_name" {
  description = "Secondary subnet range name for GKE pods"
  type        = string
}

variable "gke_services_secondary_range_name" {
  description = "Secondary subnet range name for GKE services"
  type        = string
}

variable "gke_release_channel" {
  description = "GKE release channel"
  type        = string
  default     = "REGULAR"
}

variable "gke_node_locations" {
  description = "Zones used by the GKE cluster/default node pool"
  type        = list(string)
  default     = []
}

variable "gke_datapath_provider" {
  description = "Datapath provider for GKE"
  type        = string
  default     = "ADVANCED_DATAPATH"
}

variable "gke_enable_shielded_nodes" {
  description = "Enable shielded nodes for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_private_nodes" {
  description = "Enable private nodes for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_private_endpoint" {
  description = "Enable private endpoint for GKE control plane"
  type        = bool
  default     = false
}

variable "gke_master_ipv4_cidr_block" {
  description = "Master IPv4 CIDR block for private GKE cluster"
  type        = string
  default     = "172.16.0.0/28"
}

variable "gke_master_authorized_networks" {
  description = "Authorized networks for GKE control plane access"
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = []
}

variable "gke_deletion_protection" {
  description = "Enable deletion protection for the GKE cluster. Set to false to allow cluster deletion via Terraform."
  type        = bool
}

variable "gke_database_encryption_key_name" {
  description = "Cloud KMS key for GKE database encryption (CMEK). Format: projects/PROJECT_ID/locations/LOCATION/keyRings/RING_NAME/cryptoKeys/KEY_NAME"
  type        = string
  default     = null
}

variable "gke_logging_components" {
  description = "Logging components to enable for GKE"
  type        = list(string)
  default     = ["SYSTEM_COMPONENTS", "WORKLOADS"]
}

variable "gke_monitoring_components" {
  description = "Monitoring components to enable for GKE"
  type        = list(string)
  default     = ["SYSTEM_COMPONENTS", "POD", "DEPLOYMENT", "STATEFULSET", "DAEMONSET"]
}

variable "gke_enable_managed_prometheus" {
  description = "Enable managed prometheus for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_http_load_balancing" {
  description = "Enable HTTP load balancing addon for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_gce_persistent_disk_csi_driver" {
  description = "Enable GCE PD CSI driver for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_gcp_filestore_csi_driver" {
  description = "Enable GCP Filestore CSI driver for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_dns_cache" {
  description = "Enable DNS cache addon for GKE"
  type        = bool
  default     = true
}

variable "gke_enable_backup" {
  description = "Enable GKE Backup for Workloads (Backup for GKE)"
  type        = bool
}

# ==============================================================================
# GKE MAINTENANCE POLICY
# ==============================================================================

variable "gke_enable_maintenance_policy" {
  description = "Enable maintenance policy for the GKE cluster"
  type        = bool
  default     = false
}

variable "gke_maintenance_exclusions" {
  description = "List of maintenance exclusion windows when no upgrades should occur"
  type = list(object({
    name       = string
    start_time = string
    end_time   = string
    scope      = string
  }))
  default = []
  validation {
    condition = alltrue([
      for exclusion in var.gke_maintenance_exclusions :
      contains(["NO_UPGRADES", "NO_MINOR_UPGRADES", "NO_MINOR_OR_NODE_UPGRADES"], exclusion.scope)
    ])
    error_message = "Exclusion scope must be one of: NO_UPGRADES, NO_MINOR_UPGRADES, NO_MINOR_OR_NODE_UPGRADES"
  }
}

variable "gke_maintenance_exclusion_duration_days" {
  description = "Duration in days for the maintenance exclusion window (used when dynamic calculation is enabled)"
  type        = number
  default     = 30
}

variable "gke_use_dynamic_maintenance_window" {
  description = "If true, calculate maintenance exclusion start time from current timestamp"
  type        = bool
  default     = false
}

variable "gke_daily_maintenance_window" {
  description = "Daily maintenance window configuration. Only start_time is configurable; duration is computed by GCP (typically 4 hours)."
  type = object({
    start_time = string
  })
  default = null
}

variable "gke_recurring_maintenance_window" {
  description = "Recurring maintenance window configuration"
  type = object({
    start_time = string
    end_time   = string
    recurrence = string
  })
  default = null
}

# ==============================================================================
# GKE NODE AUTO-PROVISIONING
# ==============================================================================

variable "gke_enable_node_auto_provisioning" {
  description = "Enable Node Auto-Provisioning (NAP) to automatically create node pools based on workload requirements"
  type        = bool
  default     = false
}

variable "gke_node_auto_provisioning_locations" {
  description = "List of zones where auto-provisioned nodes can be created"
  type        = list(string)
  default     = []
}

variable "gke_node_autoscaling_profile" {
  description = "Autoscaling profile for NAP: BALANCED or OPTIMIZE_UTILIZATION"
  type        = string
  default     = "BALANCED"
  validation {
    condition     = contains(["BALANCED", "OPTIMIZE_UTILIZATION"], var.gke_node_autoscaling_profile)
    error_message = "Autoscaling profile must be either BALANCED or OPTIMIZE_UTILIZATION"
  }
}

variable "gke_node_auto_provisioning_limits" {
  description = "Resource limits for Node Auto-Provisioning"
  type = object({
    cpu_min    = number
    cpu_max    = number
    memory_min = number
    memory_max = number
  })
  default = {
    cpu_min    = 2
    cpu_max    = 500
    memory_min = 4
    memory_max = 3000
  }
}

variable "gke_node_auto_provisioning_defaults" {
  description = "Default configuration for auto-provisioned node pools"
  type = object({
    disk_size_gb       = number
    disk_type          = string
    image_type         = string
    min_cpu_platform   = string
    boot_disk_kms_key  = string
    enable_secure_boot = bool
    max_surge          = number
    max_unavailable    = number
    upgrade_strategy   = string
  })
  default = {
    disk_size_gb       = 100
    disk_type          = "pd-standard"
    image_type         = "COS_CONTAINERD"
    min_cpu_platform   = null
    boot_disk_kms_key  = null
    enable_secure_boot = false
    max_surge          = 1
    max_unavailable    = 0
    upgrade_strategy   = "SURGE"
  }
}

variable "gke_default_node_pool" {
  description = "Default/infra node pool configuration for GKE"
  type = object({
    name               = string
    initial_node_count = number
    min_node_count     = number
    max_node_count     = number
    max_pods_per_node  = number
    node_locations     = list(string)
    version            = string
    auto_repair        = bool
    auto_upgrade       = bool
    service_account    = string
    machine_type       = string
    disk_size_gb       = number
    disk_type          = string
    image_type         = string
    oauth_scopes       = list(string)
    tags               = list(string)
    labels             = map(string)
    spot               = bool
    enable_secure_boot = bool
  })
}

# ==============================================================================
# OPTIONAL VARIABLES - GCS Buckets and Paths
# ==============================================================================

variable "function_source_bucket" {
  description = "GCS bucket name containing Cloud Function source code"
  type        = string
  default     = "byoc-terraform"
}

variable "workflow_source_bucket" {
  description = "GCS bucket name containing Workflow YAML files"
  type        = string
  default     = "byoc-terraform"
}

# Cloud Function source code paths
variable "send_dataplane_status_source_object" {
  description = "GCS object path for send_dataplane_status function source"
  type        = string
  default     = "functions/send_dataplane_status.zip"
}

variable "kube_api_proxy_source_object" {
  description = "GCS object path for kube_api_proxy function source"
  type        = string
  default     = "functions/kube_api_proxy.zip"
}

variable "install_operator_source_object" {
  description = "GCS object path for install_operator function source"
  type        = string
  default     = "functions/install_operator.zip"
}

variable "get_operator_status_source_object" {
  description = "GCS object path for get_operator_status function source"
  type        = string
  default     = "functions/get_operator_status.zip"
}

variable "common_serverless_source_object" {
  description = "GCS object path for common_serverless function source (orchestrator)"
  type        = string
  default     = "functions/common_serverless.zip"
}

# Workflow YAML file paths
variable "cluster_setup_workflow_object" {
  description = "GCS object path for cluster setup workflow YAML"
  type        = string
}

variable "delete_cluster_setup_workflow_object" {
  description = "GCS object path for delete cluster setup workflow YAML"
  type        = string
}

variable "engine_deploy_workflow_object" {
  description = "GCS object path for engine deploy workflow YAML"
  type        = string
}

# ==============================================================================
# OPTIONAL VARIABLES - Cloud Functions Configuration
# ==============================================================================

variable "python_runtime" {
  description = "Python runtime version for Cloud Functions"
  type        = string
  default     = "python311"
}

variable "function_memory_mb" {
  description = "Memory allocation for Cloud Functions"
  type        = string
  default     = "512Mi"
}

variable "function_timeout_seconds" {
  description = "Timeout for Cloud Functions in seconds"
  type        = number
  default     = 540
}

variable "function_max_instances" {
  description = "Maximum number of Cloud Function instances"
  type        = number
  default     = 10
}

variable "function_min_instances" {
  description = "Minimum number of Cloud Function instances"
  type        = number
  default     = 0
}

# ==============================================================================
# OPTIONAL VARIABLES - Environment Variables
# ==============================================================================

variable "function_common_env_vars" {
  description = "Common environment variables for all Cloud Functions"
  type        = map(string)
  default     = {}
}

variable "send_dataplane_status_env_vars" {
  description = "Environment variables for send_dataplane_status function"
  type        = map(string)
  default     = {}
}

variable "kube_api_proxy_env_vars" {
  description = "Environment variables for kube_api_proxy function"
  type        = map(string)
  default     = {}
}

variable "install_operator_env_vars" {
  description = "Environment variables for install_operator function"
  type        = map(string)
  default     = {}
}

variable "get_operator_status_env_vars" {
  description = "Environment variables for get_operator_status function"
  type        = map(string)
  default     = {}
}

variable "common_serverless_env_vars" {
  description = "Environment variables for common_serverless function (orchestrator)"
  type        = map(string)
  default     = {}
}

# ==============================================================================
# OPTIONAL VARIABLES - CMEK CONFIGURATION
# ==============================================================================

variable "workflows_kms_key" {
  description = "Optional Cloud KMS crypto key resource name for Workflows state data encryption."
  type        = string
  default     = null
}

# ==============================================================================
# OPTIONAL VARIABLES - Pub/Sub Configuration
# ==============================================================================

variable "input_queue_ack_deadline_seconds" {
  description = "Acknowledgement deadline for input queue subscription in seconds"
  type        = number
  default     = 600
}

variable "max_delivery_attempts" {
  description = "Maximum number of delivery attempts before sending to dead letter queue"
  type        = number
  default     = 5
}

# ==============================================================================
# VPC Configuration
# ==============================================================================

variable "vpc_routing_mode" {
  description = "Network routing mode (REGIONAL or GLOBAL)"
  type        = string
  default     = "REGIONAL"
}

variable "vpc_mtu" {
  description = "Maximum Transmission Unit in bytes (1460-8896)"
  type        = number
  default     = 8896
}

variable "vpc_subnets" {
  description = "Map of subnets to create. Names are auto-generated from naming_prefix; each map key is used to derive the subnet name."
  type = map(object({
    region                   = string
    ip_cidr_range            = string
    private_ip_google_access = bool
    description              = string
    enable_flow_logs         = bool
    flow_logs_config = object({
      aggregation_interval = string
      flow_sampling        = number
      metadata             = string
    })
    secondary_ip_ranges = list(object({
      range_name    = string
      ip_cidr_range = string
    }))
  }))
}

variable "create_nat_gateway" {
  description = "Create Cloud NAT gateway for outbound internet access"
  type        = bool
  default     = true
}

variable "router_asn" {
  description = "BGP ASN for Cloud Router"
  type        = number
  default     = 64514
}

variable "nat_log_config" {
  description = "Cloud NAT logging configuration"
  type = object({
    enable = bool
    filter = string
  })
  default = {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

variable "create_default_firewall_rules" {
  description = "Create default firewall rules (allow internal, allow IAP SSH)"
  type        = bool
  default     = true
}

# ==============================================================================
# VPC Connector Configuration
# ==============================================================================

variable "create_vpc_connector" {
  description = "Create VPC Access Connector for Cloud Functions"
  type        = bool
  default     = true
}

variable "vpc_connector_name" {
  description = "Name of the VPC Access Connector."
  type        = string
  default     = null
}

variable "vpc_connector_ip_cidr_range" {
  description = "CIDR range for VPC connector (must be /28 and not overlap with existing ranges)"
  type        = string
  default     = "10.8.0.0/28"
}

variable "vpc_connector_min_instances" {
  description = "Minimum number of VPC connector instances"
  type        = number
  default     = 2
}

variable "vpc_connector_max_instances" {
  description = "Maximum number of VPC connector instances"
  type        = number
  default     = 3
}

variable "vpc_connector_machine_type" {
  description = "Machine type for VPC connector instances"
  type        = string
  default     = "e2-micro"
}

variable "vpc_connector_egress_settings" {
  description = "VPC egress settings for Cloud Functions. Use 'ALL_TRAFFIC' to route all traffic through VPC, or 'PRIVATE_RANGES_ONLY' for only private IP ranges (recommended for private GKE)"
  type        = string
  default     = "PRIVATE_RANGES_ONLY"
  validation {
    condition     = contains(["ALL_TRAFFIC", "PRIVATE_RANGES_ONLY"], var.vpc_connector_egress_settings)
    error_message = "vpc_connector_egress_settings must be either 'ALL_TRAFFIC' or 'PRIVATE_RANGES_ONLY'"
  }
}