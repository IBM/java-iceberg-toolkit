# ==============================================================================
# IAM Configuration for Pub/Sub Module
# ==============================================================================

# ------------------------------------------------------------------------------
# Status Topic Publisher - send_dataplane_status function
# ------------------------------------------------------------------------------
resource "google_pubsub_topic_iam_member" "status_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.ibm_byoc_dataplane_status_topic.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.send_dataplane_status_sa}"
}


