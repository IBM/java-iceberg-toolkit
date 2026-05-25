# ==============================================================================
# Pub/Sub Module
# ==============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ==============================================================================
# Topic 1: Request Queue Topic
# ==============================================================================

resource "google_pubsub_topic" "ibm_byoc_input_topic" {
  name    = "${var.naming_prefix}-topic-input-queue"
  project = var.project_id

  # Message retention for 7 days
  message_retention_duration = "604800s"

  # Enable message ordering
  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }

  labels = var.labels
}


# Dead Letter Queue for input queue
resource "google_pubsub_topic" "ibm_byoc_input_topic_dlq" {
  name    = "${var.naming_prefix}-topic-input-queue-dlq"
  project = var.project_id

  message_retention_duration = "604800s"
  labels                     = var.labels
}

# Subscription for DLQ
resource "google_pubsub_subscription" "ibm_byoc_input_topic_dlq_sub" {
  name    = "${var.naming_prefix}-sub-input-queue-dlq"
  topic   = google_pubsub_topic.ibm_byoc_input_topic_dlq.name
  project = var.project_id

  # Pull subscription for manual inspection
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
}

# ==============================================================================
# Topic 2: Response Topic 
# Receives status updates from send_dataplane_status function
# ==============================================================================

resource "google_pubsub_topic" "ibm_byoc_dataplane_status_topic" {
  name    = "${var.naming_prefix}-topic-dataplane-status"
  project = var.project_id

  # Message retention for 7 days
  message_retention_duration = "604800s"

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }

  labels = var.labels
}

# Subscription for control plane to receive status notifications
resource "google_pubsub_subscription" "ibm_byoc_dataplane_status_sub" {
  name    = "${var.naming_prefix}-sub-dataplane-status"
  topic   = google_pubsub_topic.ibm_byoc_dataplane_status_topic.name
  project = var.project_id

  # Pull subscription for control plane to consume
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Dead letter for failed status notifications
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.ibm_byoc_dataplane_status_dlq.id
    max_delivery_attempts = 5
  }
}

# Dead Letter Queue for status notifications
resource "google_pubsub_topic" "ibm_byoc_dataplane_status_dlq" {
  name    = "${var.naming_prefix}-topic-dataplane-status-dlq"
  project = var.project_id

  message_retention_duration = "604800s"
  labels                     = var.labels
}

# Subscription for status DLQ
resource "google_pubsub_subscription" "ibm_byoc_dataplane_status_dlq_sub" {
  name    = "${var.naming_prefix}-sub-dataplane-status-dlq"
  topic   = google_pubsub_topic.ibm_byoc_dataplane_status_dlq.name
  project = var.project_id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
}


