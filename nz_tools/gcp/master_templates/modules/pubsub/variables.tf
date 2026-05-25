# ==============================================================================
# Variables for GCP Pub/Sub Module
# ==============================================================================

variable "project_id" {
  description = "GCP Project ID where Pub/Sub resources will be created"
  type        = string
}

variable "region" {
  description = "GCP region for Pub/Sub resources"
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

variable "send_dataplane_status_sa" {
  description = "Service account email for send_dataplane_status function that publishes to status topic"
  type        = string
}

variable "ack_deadline_seconds" {
  description = "Acknowledgment deadline for subscriptions in seconds"
  type        = number
  default     = 600
}

variable "max_delivery_attempts" {
  description = "Maximum number of delivery attempts before sending to DLQ"
  type        = number
  default     = 5
}

variable "backlog_alert_threshold" {
  description = "Threshold for undelivered messages alert"
  type        = number
  default     = 100
}

variable "notification_channels" {
  description = "List of notification channel IDs for alerting"
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}