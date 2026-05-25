# ==============================================================================
# IAM Configuration for Cloud Functions Module
# ==============================================================================
# GCS Permissions - Read function source code from bucket
# ------------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "function_source_reader" {
  bucket = var.function_source_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.ops_sa_email}"
}

# ------------------------------------------------------------------------------
# Cloud Functions Invoker - Allow Pub/Sub to invoke common_serverless function
# ------------------------------------------------------------------------------
resource "google_cloudfunctions2_function_iam_member" "common_serverless_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.ibm_byoc_common_serverless_function.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${var.pubsub_invoker_service_account}"
}
# ------------------------------------------------------------------------------




