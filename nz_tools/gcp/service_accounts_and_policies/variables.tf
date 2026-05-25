# ==============================================================================
# Service Accounts and Policies - Root Module Variables
# ==============================================================================

# ==============================================================================
# REQUIRED VARIABLES
# ==============================================================================

variable "project_id" {
  description = "GCP Project ID where service accounts will be created"
  type        = string
}

variable "region" {
  description = "GCP region for resource deployment"
  type        = string
}

variable "dataplane_id" {
  description = "Unique identifier for the dataplane (naming_prefix will be constructed as ibm-byoc-{dataplane_id})"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.dataplane_id))
    error_message = "dataplane_id must contain only lowercase alphanumeric characters and hyphens"
  }
}

variable "dataplane_crn" {
  description = "Cloud Resource Name for the dataplane"
  type        = string
}

# ==============================================================================
# OPTIONAL VARIABLES
# ==============================================================================

variable "byoc_engine_label" {
  description = "BYOC engine label for resource labeling"
  type        = string
  default     = "netezza"
}

variable "terraform_source_bucket" {
  description = "GCS bucket name containing Terraform source code (Infrastructure Manager requires read access)"
  type        = string
}

variable "infra_manager_artifacts_bucket" {
  description = "GCS bucket name for Infrastructure Manager artifacts and state (Infrastructure Manager requires read/write access)"
  type        = string
}