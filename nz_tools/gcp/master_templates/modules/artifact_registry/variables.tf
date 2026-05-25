# ==============================================================================
# Artifact Registry Module Variables
# ==============================================================================

# ==============================================================================
# REQUIRED VARIABLES
# ==============================================================================

variable "project_id" {
  description = "GCP project ID where Artifact Registry will be created"
  type        = string
}

variable "region" {
  description = "GCP region for Artifact Registry"
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

variable "ops_sa_email" {
  description = "Operations service account email with read/write access to registry"
  type        = string
}

# ==============================================================================
# OPTIONAL VARIABLES - Configuration
# ==============================================================================

variable "enable_immutable_tags" {
  description = "Enable immutable tags to prevent tag overwrites"
  type        = bool
  default     = true
}

variable "untagged_image_retention_days" {
  description = "Number of days to retain untagged images before deletion"
  type        = number
  default     = 30
}

variable "keep_recent_versions" {
  description = "Number of recent image versions to keep"
  type        = number
  default     = 10
}

variable "enable_vulnerability_scanning" {
  description = "Enable vulnerability scanning for container images"
  type        = bool
  default     = true
}

variable "gke_sa_email" {
  description = "GKE service account email for read-only access (optional)"
  type        = string
  default     = null
}

variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}

# ==============================================================================
# OPTIONAL VARIABLES - Networking (Future Enhancement)
# ==============================================================================

variable "vpc_id" {
  description = "VPC ID for private connectivity (future enhancement)"
  type        = string
  default     = null
}