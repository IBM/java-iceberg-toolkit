# ==============================================================================
# IAM Policies for Artifact Registry
# ==============================================================================
# Access Control: Ops service account ONLY
# Ops Service Account - Read/Write Access
# Purpose: Allow ops SA to pull and push Netezza container images
# Scope: Dataplane operations only
# ------------------------------------------------------------------------------

resource "google_artifact_registry_repository_iam_member" "ops_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.ibm_byoc_netezza_registry.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${var.ops_sa_email}"
}

resource "google_artifact_registry_repository_iam_member" "ops_writer" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.ibm_byoc_netezza_registry.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${var.ops_sa_email}"
}

# ------------------------------------------------------------------------------
# GKE Service Account - Read-Only Access 
# ------------------------------------------------------------------------------
# Purpose: Allow GKE nodes to pull images for Netezza deployment
# Scope: Read-only access for container runtime
# ------------------------------------------------------------------------------

resource "google_artifact_registry_repository_iam_member" "gke_reader" {
  count = var.gke_sa_email != null ? 1 : 0

  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.ibm_byoc_netezza_registry.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${var.gke_sa_email}"
}