# ==============================================================================
# Variables for GCP Workflows Module
# ==============================================================================

variable "project_id" {
  description = "GCP Project ID where workflows will be deployed"
  type        = string
}

variable "region" {
  description = "GCP region for workflow deployment"
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

variable "workflow_definitions_bucket" {
  description = "GCS bucket name containing workflow YAML definitions"
  type        = string
}


# ==============================================================================
# WORKFLOW DEFINITION FILES
# ==============================================================================

variable "cluster_setup_workflow_file" {
  description = "GCS object path for cluster setup workflow YAML"
  type        = string
}

variable "delete_cluster_setup_workflow_file" {
  description = "GCS object path for delete cluster setup workflow YAML"
  type        = string
}

variable "netezza_engine_deploy_workflow_file" {
  description = "GCS object path for Netezza engine deploy workflow YAML"
  type        = string
}

# ------------------------------------------------------------------------------
# SERVICE ACCOUNT
# ------------------------------------------------------------------------------
# Service account used by Workflows for runtime execution
# ------------------------------------------------------------------------------

variable "ops_sa_email" {
  description = "Operations service account email - Used by Workflows for runtime execution."
  type        = string
}

variable "kms_key_name" {
  description = "Optional Cloud KMS crypto key resource name for Workflows state data encryption."
  type        = string
  default     = null
}
variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}
