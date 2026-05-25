# ==============================================================================
# Outputs for GCP Service Accounts Module
# ==============================================================================

# Infrastructure Service Account Outputs
output "infra_sa_email" {
  description = "Email of the Infrastructure service account"
  value       = google_service_account.ibm_byoc_infra_sa.email
}

output "infra_sa_id" {
  description = "ID of the Infrastructure service account"
  value       = google_service_account.ibm_byoc_infra_sa.id
}

# Operations Service Account Outputs
output "ops_sa_email" {
  description = "Email of the Operations service account"
  value       = google_service_account.ibm_byoc_ops_sa.email
}

output "ops_sa_id" {
  description = "ID of the Operations service account"
  value       = google_service_account.ibm_byoc_ops_sa.id
}

# Cluster Operator Service Account Outputs
output "cluster_operator_sa_email" {
  description = "Email of the Cluster Operator service account"
  value       = google_service_account.ibm_byoc_cluster_operator_sa.email
}

output "cluster_operator_sa_id" {
  description = "ID of the Cluster Operator service account"
  value       = google_service_account.ibm_byoc_cluster_operator_sa.id
}

# Summary Output
output "all_service_accounts" {
  description = "Map of all service account emails"
  value = {
    infra_sa            = google_service_account.ibm_byoc_infra_sa.email
    ops_sa              = google_service_account.ibm_byoc_ops_sa.email
    cluster_operator_sa = google_service_account.ibm_byoc_cluster_operator_sa.email
  }
}
