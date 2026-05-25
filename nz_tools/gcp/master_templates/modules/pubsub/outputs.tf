# ==============================================================================
# Outputs for GCP Pub/Sub Module
# ==============================================================================

# Input Queue Outputs
output "input_queue_topic_id" {
  description = "ID of the input queue topic"
  value       = google_pubsub_topic.ibm_byoc_input_topic.id
}

output "input_queue_topic_name" {
  description = "Name of the input queue topic"
  value       = google_pubsub_topic.ibm_byoc_input_topic.name
}


output "input_queue_dlq_topic_id" {
  description = "ID of the input queue DLQ topic"
  value       = google_pubsub_topic.ibm_byoc_input_topic_dlq.id
}

output "input_queue_dlq_topic_name" {
  description = "Name of the input queue DLQ topic"
  value       = google_pubsub_topic.ibm_byoc_input_topic_dlq.name
}

# Dataplane Status Outputs
output "dataplane_status_topic_id" {
  description = "ID of the dataplane status topic"
  value       = google_pubsub_topic.ibm_byoc_dataplane_status_topic.id
}

output "dataplane_status_topic_name" {
  description = "Name of the dataplane status topic"
  value       = google_pubsub_topic.ibm_byoc_dataplane_status_topic.name
}

output "dataplane_status_subscription_id" {
  description = "ID of the dataplane status subscription"
  value       = google_pubsub_subscription.ibm_byoc_dataplane_status_sub.id
}

output "dataplane_status_subscription_name" {
  description = "Name of the dataplane status subscription"
  value       = google_pubsub_subscription.ibm_byoc_dataplane_status_sub.name
}

output "dataplane_status_dlq_topic_id" {
  description = "ID of the dataplane status DLQ topic"
  value       = google_pubsub_topic.ibm_byoc_dataplane_status_dlq.id
}

output "dataplane_status_dlq_topic_name" {
  description = "Name of the dataplane status DLQ topic"
  value       = google_pubsub_topic.ibm_byoc_dataplane_status_dlq.name
}

# Summary Output
output "all_topics" {
  description = "Map of all Pub/Sub topic names"
  value = {
    input_queue          = google_pubsub_topic.ibm_byoc_input_topic.name
    input_queue_dlq      = google_pubsub_topic.ibm_byoc_input_topic_dlq.name
    dataplane_status     = google_pubsub_topic.ibm_byoc_dataplane_status_topic.name
    dataplane_status_dlq = google_pubsub_topic.ibm_byoc_dataplane_status_dlq.name
  }
}

output "all_subscriptions" {
  description = "Map of all Pub/Sub subscription names (input_queue managed by Eventarc)"
  value = {
    input_queue_dlq      = google_pubsub_subscription.ibm_byoc_input_topic_dlq_sub.name
    dataplane_status     = google_pubsub_subscription.ibm_byoc_dataplane_status_sub.name
    dataplane_status_dlq = google_pubsub_subscription.ibm_byoc_dataplane_status_dlq_sub.name
  }
}