# ==============================================================================
# IAM Resources for GCP Service Accounts Module
# ==============================================================================

# ==============================================================================
# 1. Infrastructure Service Account IAM Resources
# ==============================================================================

# Infrastructure service account roles - using for_each for cleaner code
locals {
  infra_sa_roles = toset([
    "roles/compute.networkAdmin",     # Network/subnet/route management (not firewalls)
    "roles/compute.securityAdmin",    # Firewall rule management
    "roles/compute.instanceAdmin",    # Instance management
    "roles/container.clusterAdmin",   # GKE cluster admin
    "roles/storage.objectAdmin",      # Storage management
    "roles/cloudfunctions.developer", # Cloud Functions management
    "roles/pubsub.editor",            # Pub/Sub management
    "roles/workflows.editor",         # Workflows management
    "roles/iam.serviceAccountUser",   # Use service accounts
    "roles/monitoring.metricWriter",  # Write metrics
    "roles/logging.logWriter",        # Write logs
    "roles/config.admin",             # Infrastructure Manager deployment
    "roles/vpcaccess.admin",          # VPC Access Connector management
    "roles/artifactregistry.admin",   # Artifact Registry repository management
    "roles/iam.securityAdmin",        # IAM policy management on resources
    "roles/cloudkms.admin"            # KMS key and IAM management
  ])
}

resource "google_project_iam_member" "ibm_byoc_infra_roles" {
  for_each = local.infra_sa_roles

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.ibm_byoc_infra_sa.email}"

}


# Infrastructure Service Account - GCS Bucket Permissions
# Grant read access to Terraform source bucket (Infrastructure Manager requirement)
resource "google_storage_bucket_iam_member" "infra_sa_terraform_source_reader" {
  bucket = var.terraform_source_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ibm_byoc_infra_sa.email}"
}

# Grant read/write access to Infrastructure Manager artifacts bucket
# Infrastructure Manager needs to store state files and deployment artifacts
resource "google_storage_bucket_iam_member" "infra_sa_artifacts_admin" {
  bucket = var.infra_manager_artifacts_bucket
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.ibm_byoc_infra_sa.email}"
}

# ==============================================================================
# 2. Operations Service Account IAM Resources
# ==============================================================================

# Operations service account roles - using for_each for cleaner code
locals {
  ops_sa_roles = toset([
    "roles/compute.instanceAdmin.v1", # Instance start/stop/reset
    "roles/container.admin",          # GKE admin access (includes namespace, RBAC, workload management)
    "roles/logging.viewer",           # View logs
    "roles/monitoring.viewer",        # View monitoring data
    "roles/workflows.invoker",        # Execute workflows
    "roles/run.invoker",              # Invoke Cloud Run services
    "roles/secretmanager.admin",      # Full Secret Manager access (read/write/manage secrets)
    "roles/config.admin",             # For updating/deploying infrastructure manager
    "roles/iam.serviceAccountUser"    # Use service accounts
  ])
}

resource "google_project_iam_member" "ibm_byoc_ops_roles" {
  for_each = local.ops_sa_roles

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.ibm_byoc_ops_sa.email}"
}

# ==============================================================================
# 3. Cluster Operator Service Account IAM Resources
# ==============================================================================

# Cluster Operator service account roles - using for_each for cleaner code
locals {
  cluster_operator_sa_roles = toset([
    "roles/compute.instanceAdmin.v1", # Manage launch templates and instances
    "roles/compute.storageAdmin",     # Manage disks
    "roles/container.developer",      # Manage GKE node pools -- remove later
    "roles/container.clusterAdmin",     
    "roles/iam.serviceAccountUser",   # Use service accounts for nodes
    "roles/logging.logWriter"         # Write logs
  ])
}

resource "google_project_iam_member" "ibm_byoc_cluster_operator_roles" {
  for_each = local.cluster_operator_sa_roles

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.ibm_byoc_cluster_operator_sa.email}"
}

