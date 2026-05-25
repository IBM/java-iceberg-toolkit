# ==============================================================================
# IAM Configuration for Workflows Module
# ==============================================================================

# ------------------------------------------------------------------------------
# GCS Permissions - Read workflow definitions from bucket
# ------------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "workflow_definitions_reader" {
  bucket = var.workflow_definitions_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.ops_sa_email}"
}
