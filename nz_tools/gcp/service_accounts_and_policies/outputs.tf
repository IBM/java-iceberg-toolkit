# ==============================================================================
# Service Accounts and Policies - Root Module Outputs
# ==============================================================================

# ==============================================================================
# Infrastructure Service Account Outputs
# ==============================================================================

output "infra_sa_email" {
  description = "Email of the Infrastructure service account"
  value       = module.service_accounts.infra_sa_email
}

output "infra_sa_id" {
  description = "ID of the Infrastructure service account"
  value       = module.service_accounts.infra_sa_id
}

# ==============================================================================
# Operations Service Account Outputs
# ==============================================================================

output "ops_sa_email" {
  description = "Email of the Operations service account"
  value       = module.service_accounts.ops_sa_email
}

output "ops_sa_id" {
  description = "ID of the Operations service account"
  value       = module.service_accounts.ops_sa_id
}

# ==============================================================================
# Cluster Operator Service Account Outputs
# ==============================================================================

output "cluster_operator_sa_email" {
  description = "Email of the Cluster Operator service account"
  value       = module.service_accounts.cluster_operator_sa_email
}

output "cluster_operator_sa_id" {
  description = "ID of the Cluster Operator service account"
  value       = module.service_accounts.cluster_operator_sa_id
}

# ==============================================================================
# Summary Output
# ==============================================================================

output "all_service_accounts" {
  description = "Map of all service account emails"
  value       = module.service_accounts.all_service_accounts
}