# ==============================================================================
# GCP Service Accounts Module
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
# Local Variables
# ==============================================================================

locals {
  # Construct naming prefix from dataplane_id
  naming_prefix = "ibm-byoc-${var.dataplane_id}"
}

# ==============================================================================
# 1. Infrastructure Service Account
# ==============================================================================

resource "google_service_account" "ibm_byoc_infra_sa" {
  account_id   = "${local.naming_prefix}-infra"
  display_name = "BYOC Infrastructure Service Account"
  description  = "Service account for infrastructure management"
  project      = var.project_id
}

# ==============================================================================
# 2. Operations Service Account
# ==============================================================================

resource "google_service_account" "ibm_byoc_ops_sa" {
  account_id   = "${local.naming_prefix}-ops"
  display_name = "BYOC Operations Service Account"
  description  = "Service account for operations and monitoring"
  project      = var.project_id
}

# ==============================================================================
# 3. Cluster Operator Service Account
# ==============================================================================

resource "google_service_account" "ibm_byoc_cluster_operator_sa" {
  account_id   = "${local.naming_prefix}-clusterops"
  display_name = "BYOC Cluster Operator Service Account"
  description  = "Service account for cluster-operator controller managing GKE node pools and launch templates"
  project      = var.project_id
}

# ==============================================================================
# Data Sources and Local Variables
# ==============================================================================

# Get project information for Cloud Build service account
data "google_project" "project" {
  project_id = var.project_id
}

locals {
  cloud_build_sa = "${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

# ==============================================================================
# IAM Impersonation Bindings
# ==============================================================================

# Allow Cloud Build to impersonate Infrastructure service account
# This enables Cloud Build to create resources using the Infrastructure SA's permissions
# without requiring service account keys
resource "google_service_account_iam_member" "ibm_byoc_cloud_build_infra_impersonation" {
  service_account_id = google_service_account.ibm_byoc_infra_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${local.cloud_build_sa}"
}

