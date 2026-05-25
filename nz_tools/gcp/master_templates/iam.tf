# ==============================================================================
# Root-Level IAM Configuration
# ==============================================================================
# Project-level IAM bindings for ops_service_account service account
# ==============================================================================

# ------------------------------------------------------------------------------
# Data Sources
# ------------------------------------------------------------------------------
# Get project number for service agent service accounts
data "google_project" "project" {
  project_id = var.project_id
}

# ------------------------------------------------------------------------------
# Logging Permissions - Write logs to Cloud Logging
# ------------------------------------------------------------------------------
# Used by: Cloud Functions and Workflows
resource "google_project_iam_member" "ops_sa_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${var.ops_service_account}"
}

# ------------------------------------------------------------------------------
# Monitoring Permissions - Write metrics to Cloud Monitoring
# ------------------------------------------------------------------------------
# Used by: Cloud Functions and Workflows
resource "google_project_iam_member" "ops_sa_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${var.ops_service_account}"
}

# ------------------------------------------------------------------------------
# Cloud Trace Permissions - Write trace data
# ------------------------------------------------------------------------------
# Used by: Cloud Functions
resource "google_project_iam_member" "ops_sa_trace_agent" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${var.ops_service_account}"
}

# ------------------------------------------------------------------------------
# GKE Node Service Account - Default Node Service Account Role
# ------------------------------------------------------------------------------
# Grant the default node service account role to the ops service account
# This role provides the minimum permissions required for GKE nodes to function properly
# Includes: logging, monitoring, and basic GKE operations
# Required to prevent "non-degraded operation" warnings in GKE console
resource "google_project_iam_member" "ops_sa_gke_default_node_service_account" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = "serviceAccount:${var.ops_service_account}"
}

# ------------------------------------------------------------------------------
# Workflows CMEK Permissions
# ------------------------------------------------------------------------------
# Grant ops service account permission to use KMS key for Workflows encryption
resource "google_kms_crypto_key_iam_member" "workflows_ops_sa_cmek" {
  count         = var.workflows_kms_key != null ? 1 : 0
  crypto_key_id = var.workflows_kms_key
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${var.ops_service_account}"
}

# Grant Workflows Service Agent permission to use KMS key
# This is required for Workflows to encrypt/decrypt workflow state
resource "google_kms_crypto_key_iam_member" "workflows_service_agent_cmek" {
  count         = var.workflows_kms_key != null ? 1 : 0
  crypto_key_id = var.workflows_kms_key
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-workflows.iam.gserviceaccount.com"
}

# Additional decrypt permission for Workflows Service Agent
# Some operations require explicit decrypt permission
resource "google_kms_crypto_key_iam_member" "workflows_service_agent_decrypt" {
  count         = var.workflows_kms_key != null ? 1 : 0
  crypto_key_id = var.workflows_kms_key
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-workflows.iam.gserviceaccount.com"
}

# ------------------------------------------------------------------------------
# GKE Database Encryption CMEK Permissions
# ------------------------------------------------------------------------------
# Grants Google's GKE service agent permission to use your KMS key for etcd encryption.
#
# IMPORTANT: Must use Google's predefined service account (service-PROJECT_NUMBER@container-engine-robot.iam.gserviceaccount.com)
# - Auto-created by Google when GKE API is enabled
# - Required for control plane operations (cannot use custom SA)
# - Provides security isolation between control plane, data plane, and workloads
# - All operations logged in Cloud Audit Logs
#
# Without this permission, cluster creation fails with "permission denied" error.

resource "google_kms_crypto_key_iam_member" "gke_database_cmek" {
  count         = var.gke_database_encryption_key_name != null ? 1 : 0
  crypto_key_id = var.gke_database_encryption_key_name
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@container-engine-robot.iam.gserviceaccount.com"
}

# Additional decrypt permission for GKE service account (required for some operations)
resource "google_kms_crypto_key_iam_member" "gke_database_cmek_decrypt" {
  count         = var.gke_database_encryption_key_name != null ? 1 : 0
  crypto_key_id = var.gke_database_encryption_key_name
  role          = "roles/cloudkms.cryptoKeyDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@container-engine-robot.iam.gserviceaccount.com"
}

# ------------------------------------------------------------------------------
# GKE Node Boot Disk Encryption CMEK Permissions
# ------------------------------------------------------------------------------
# Grants Google's Compute Engine service agent permission to use your KMS key for node boot disk encryption.
#
# IMPORTANT: Must use Google's predefined service account (service-PROJECT_NUMBER@compute-system.iam.gserviceaccount.com)
# - Auto-created by Google when Compute Engine API is enabled
# - Required for encrypting node VM boot disks
# - Separate from the GKE service agent (container-engine-robot)

resource "google_kms_crypto_key_iam_member" "gke_node_disk_cmek" {
  count         = var.gke_database_encryption_key_name != null ? 1 : 0
  crypto_key_id = var.gke_database_encryption_key_name
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@compute-system.iam.gserviceaccount.com"
}

# ------------------------------------------------------------------------------
# Cloud Build Service Account Permissions
# ------------------------------------------------------------------------------
# Grants Cloud Build service account permissions to build Cloud Functions
# Required for Cloud Functions deployment to succeed

resource "google_project_iam_member" "cloudbuild_sa_logs_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

resource "google_project_iam_member" "cloudbuild_sa_artifact_registry_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Cloud Build needs broader permissions - using Cloud Build Service Account role
resource "google_project_iam_member" "cloudbuild_sa_service_account" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Storage object viewer for reading function source code
resource "google_project_iam_member" "cloudbuild_sa_storage_object_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Compute Engine default service account also needs access (used by Cloud Build in some configurations)
resource "google_storage_bucket_iam_member" "compute_sa_gcf_sources_user" {
  bucket = "gcf-v2-sources-${data.google_project.project.number}-${var.region}"
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "compute_sa_gcf_sources_viewer" {
  bucket = "gcf-v2-sources-${data.google_project.project.number}-${var.region}"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}


################################DELETE BELOW ALTER AFTER TESTING IN DIFFRENT REGION IF EVERYTHING WORKS COMMENTING FOR NOW#############################################################
# ------------------------------------------------------------------------------
# Cloud Build Bucket Access for Cloud Functions Source Code
# ------------------------------------------------------------------------------
# Cloud Build needs read/write access to function source code in GCS buckets
# The gcf-v2-sources bucket is auto-created by Cloud Functions but needs explicit IAM
# Using objectUser role (includes read + write) to match working us-central1 configuration

# resource "google_storage_bucket_iam_member" "cloudbuild_sa_gcf_sources_user" {
#  bucket = "gcf-v2-sources-${data.google_project.project.number}-${var.region}"
#  role   = "roles/storage.objectUser"
#  member = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
#}

# resource "google_storage_bucket_iam_member" "cloudbuild_sa_gcf_sources_viewer" {
#  bucket = "gcf-v2-sources-${data.google_project.project.number}-${var.region}"
#  role   = "roles/storage.objectViewer"
#  member = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
#}

#resource "google_storage_bucket_iam_member" "cloudbuild_service_agent_gcf_sources_viewer" {
#  bucket = "gcf-v2-sources-${data.google_project.project.number}-${var.region}"
#  role   = "roles/storage.objectViewer"
#  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
#}



