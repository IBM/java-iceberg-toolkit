# ==============================================================================
# Cloud Functions Module Variables
# ==============================================================================

# ==============================================================================
# REQUIRED VARIABLES
# ==============================================================================

variable "project_id" {
  description = "GCP project ID where Cloud Functions will be deployed"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Functions deployment"
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

variable "function_source_bucket" {
  description = "GCS bucket name containing Cloud Function source code"
  type        = string
}

variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}


# ------------------------------------------------------------------------------
# SERVICE ACCOUNT
# ------------------------------------------------------------------------------
# Service account used by Cloud Functions for runtime execution
# ------------------------------------------------------------------------------

variable "ops_sa_email" {
  description = "Operations service account email - Used by Cloud Functions for runtime execution."
  type        = string
}

# ==============================================================================
# OPTIONAL VARIABLES - Runtime Configuration
# ==============================================================================

variable "python_runtime" {
  description = "Python runtime version for Cloud Functions"
  type        = string
  default     = "python311"
}

variable "memory_mb" {
  description = "Memory allocation for Cloud Functions in MB"
  type        = string
  default     = "512Mi"
}

variable "timeout_seconds" {
  description = "Timeout for Cloud Functions in seconds"
  type        = number
  default     = 540
}

variable "max_instance_count" {
  description = "Maximum number of function instances"
  type        = number
  default     = 10
}

variable "min_instance_count" {
  description = "Minimum number of function instances"
  type        = number
  default     = 0
}

# ==============================================================================
# OPTIONAL VARIABLES - Source Code Objects
# ==============================================================================

variable "send_dataplane_status_source_object" {
  description = "GCS object path for send_dataplane_status function source code"
  type        = string
}

variable "kube_api_proxy_source_object" {
  description = "GCS object path for kube_api_proxy function source code"
  type        = string
}

variable "install_operator_source_object" {
  description = "GCS object path for install_operator function source code"
  type        = string
}

variable "get_operator_status_source_object" {
  description = "GCS object path for get_operator_status function source code"
  type        = string
}

variable "common_serverless_source_object" {
  description = "GCS object path for common_serverless function source code (orchestrator)"
  type        = string
}

# ==============================================================================
# OPTIONAL VARIABLES - Environment Variables
# ==============================================================================

variable "common_env_vars" {
  description = "Common environment variables for all Cloud Functions"
  type        = map(string)
  default     = {}
}

variable "send_dataplane_status_env_vars" {
  description = "Environment variables specific to send_dataplane_status function"
  type        = map(string)
  default     = {}
}

variable "kube_api_proxy_env_vars" {
  description = "Environment variables specific to kube_api_proxy function"
  type        = map(string)
  default     = {}
}

variable "install_operator_env_vars" {
  description = "Environment variables specific to install_operator function"
  type        = map(string)
  default     = {}
}

variable "get_operator_status_env_vars" {
  description = "Environment variables specific to get_operator_status function"
  type        = map(string)
  default     = {}
}

variable "common_serverless_env_vars" {
  description = "Environment variables specific to common_serverless function (orchestrator)"
  type        = map(string)
  default     = {}
}

# ==============================================================================
# OPTIONAL VARIABLES - Pub/Sub Integration
# ==============================================================================

variable "pubsub_invoker_service_account" {
  description = "Service account email that Pub/Sub uses to invoke Cloud Functions"
  type        = string
}

variable "input_queue_topic_id" {
  description = "Full resource ID of the Pub/Sub input queue topic for Eventarc trigger (e.g., projects/PROJECT_ID/topics/TOPIC_NAME)"
  type        = string
}

# ==============================================================================
# OPTIONAL VARIABLES - VPC Configuration
# ==============================================================================

variable "vpc_connector_egress_settings" {
  description = "VPC egress settings for Cloud Functions. Use 'ALL_TRAFFIC' to route all traffic through VPC, or 'PRIVATE_RANGES_ONLY' for only private IP ranges"
  type        = string
  default     = null
  validation {
    condition     = var.vpc_connector_egress_settings == null || contains(["ALL_TRAFFIC", "PRIVATE_RANGES_ONLY"], var.vpc_connector_egress_settings)
    error_message = "vpc_connector_egress_settings must be either 'ALL_TRAFFIC' or 'PRIVATE_RANGES_ONLY'"
  }
}

variable "vpc_connector" {
  description = "VPC Access Connector ID for Cloud Functions (e.g., 'projects/PROJECT_ID/locations/REGION/connectors/CONNECTOR_NAME')"
  type        = string
  default     = null
}