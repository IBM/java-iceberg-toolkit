variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}

variable "project_id" {
  description = "GCP project ID where GKE will be deployed"
  type        = string
}

variable "region" {
  description = "GCP region for cluster deployment"
  type        = string
}

variable "naming_prefix" {
  description = "Naming prefix for all resources (format: ibm-byoc-{dataplane_id})"
  type        = string

  validation {
    condition     = can(regex("^ibm-byoc-[a-z0-9-]+$", var.naming_prefix))
    error_message = "naming_prefix must start with 'ibm-byoc-' followed by lowercase alphanumeric characters and hyphens"
  }
}

variable "dataplane_id" {
  description = "Unique identifier for the dataplane"
  type        = string
}

variable "dataplane_crn" {
  description = "Cloud Resource Name for the dataplane - Optional, used for reference only"
  type        = string
  default     = ""
}

variable "byoc_engine_label" {
  description = "BYOC engine label for resource labeling"
  type        = string
  default     = "netezza"
}

variable "location" {
  description = "GCP region or zone for GKE cluster deployment. Can be a region (e.g., us-central1) for regional cluster or zone (e.g., us-central1-a) for zonal cluster"
  type        = string
}

variable "description" {
  description = "Description for the GKE cluster"
  type        = string
  default     = "IBM BYOC GKE cluster"
}

variable "network" {
  description = "VPC network self link or name"
  type        = string
}

variable "subnetwork" {
  description = "Subnetwork self link or name"
  type        = string
}

variable "pods_secondary_range_name" {
  description = "Secondary subnet range name for Pods"
  type        = string
}

variable "services_secondary_range_name" {
  description = "Secondary subnet range name for Services"
  type        = string
}

variable "release_channel" {
  description = "GKE release channel. Set to null or empty string to disable release channel (required for autoUpgrade: false)"
  type        = string
  default     = null
}

variable "node_locations" {
  description = "Zones for the cluster/default node pool"
  type        = list(string)
  default     = []
}

variable "datapath_provider" {
  description = "Datapath provider for GKE"
  type        = string
  default     = "ADVANCED_DATAPATH"
}

variable "enable_shielded_nodes" {
  description = "Enable shielded nodes"
  type        = bool
  default     = true
}

variable "enable_private_nodes" {
  description = "Enable private nodes"
  type        = bool
  default     = true
}

variable "enable_private_endpoint" {
  description = "Enable private control plane endpoint"
  type        = bool
  default     = false
}

variable "master_ipv4_cidr_block" {
  description = "CIDR block for the GKE control plane"
  type        = string
  default     = "172.16.0.0/28"
}

variable "master_authorized_networks" {
  description = "Authorized networks for control plane access"
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = []
}

variable "deletion_protection" {
  description = "Enable deletion protection for the GKE cluster. Set to false to allow cluster deletion."
  type        = bool
}

variable "database_encryption_key_name" {
  description = "The Cloud KMS key name to use for database encryption. Format: projects/PROJECT_ID/locations/LOCATION/keyRings/RING_NAME/cryptoKeys/KEY_NAME"
  type        = string
  default     = null
}

variable "logging_components" {
  description = "Logging components to enable"
  type        = list(string)
  default     = ["SYSTEM_COMPONENTS", "WORKLOADS"]
}

variable "monitoring_components" {
  description = "Monitoring components to enable"
  type        = list(string)
  default     = ["SYSTEM_COMPONENTS", "POD", "DEPLOYMENT", "STATEFULSET", "DAEMONSET"]
}

variable "enable_managed_prometheus" {
  description = "Enable managed prometheus"
  type        = bool
  default     = true
}

variable "enable_http_load_balancing" {
  description = "Enable HTTP load balancing addon"
  type        = bool
  default     = true
}

variable "enable_gce_persistent_disk_csi_driver" {
  description = "Enable GCE PD CSI driver"
  type        = bool
  default     = true
}

variable "enable_gcp_filestore_csi_driver" {
  description = "Enable GCP Filestore CSI driver"
  type        = bool
  default     = true
}

variable "enable_dns_cache" {
  description = "Enable DNS cache addon"
  type        = bool
  default     = true
}

variable "enable_gke_backup" {
  description = "Enable GKE Backup for Workloads (Backup for GKE)"
  type        = bool
}

# ==============================================================================
# MAINTENANCE POLICY CONFIGURATION
# ==============================================================================

variable "enable_maintenance_policy" {
  description = "Enable maintenance policy for the GKE cluster"
  type        = bool
  default     = false
}

variable "maintenance_exclusions" {
  description = "List of maintenance exclusion windows when no upgrades should occur. Use 'dynamic' for start_time to calculate from current time."
  type = list(object({
    name       = string
    start_time = string
    end_time   = string
    scope      = string
  }))
  default = []
  validation {
    condition = alltrue([
      for exclusion in var.maintenance_exclusions :
      contains(["NO_UPGRADES", "NO_MINOR_UPGRADES", "NO_MINOR_OR_NODE_UPGRADES"], exclusion.scope)
    ])
    error_message = "Exclusion scope must be one of: NO_UPGRADES, NO_MINOR_UPGRADES, NO_MINOR_OR_NODE_UPGRADES"
  }
}

variable "maintenance_exclusion_duration_days" {
  description = "Duration in days for the maintenance exclusion window (used when dynamic calculation is enabled)"
  type        = number
  default     = 30
}

variable "use_dynamic_maintenance_window" {
  description = "If true, calculate maintenance exclusion start time from current timestamp"
  type        = bool
  default     = false
}

variable "daily_maintenance_window" {
  description = "Daily maintenance window configuration. Only start_time is configurable; duration is computed by GCP (typically 4 hours)."
  type = object({
    start_time = string
  })
  default = null
}

variable "recurring_maintenance_window" {
  description = "Recurring maintenance window configuration"
  type = object({
    start_time = string
    end_time   = string
    recurrence = string
  })
  default = null
}

# ==============================================================================
# NODE AUTO-PROVISIONING CONFIGURATION
# ==============================================================================

variable "enable_node_auto_provisioning" {
  description = "Enable Node Auto-Provisioning (NAP) to automatically create node pools based on workload requirements"
  type        = bool
  default     = false
}

variable "node_auto_provisioning_locations" {
  description = "List of zones where auto-provisioned nodes can be created"
  type        = list(string)
  default     = []
}

variable "node_autoscaling_profile" {
  description = "Autoscaling profile for NAP: BALANCED or OPTIMIZE_UTILIZATION"
  type        = string
  default     = "BALANCED"
  validation {
    condition     = contains(["BALANCED", "OPTIMIZE_UTILIZATION"], var.node_autoscaling_profile)
    error_message = "Autoscaling profile must be either BALANCED or OPTIMIZE_UTILIZATION"
  }
}

variable "node_auto_provisioning_limits" {
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

variable "node_auto_provisioning_defaults" {
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

variable "default_node_pool" {
  description = "Configuration for the default/infra node pool"
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
    spot               = bool
    enable_secure_boot = bool
    boot_disk_kms_key  = optional(string, null)
  })
}

