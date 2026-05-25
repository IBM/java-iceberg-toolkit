# ==============================================================================
# Outputs for GCP Workflows Module
# ==============================================================================

output "cluster_setup_workflow_id" {
  description = "ID of the cluster setup workflow"
  value       = google_workflows_workflow.ibm_byoc_netezza_cluster_setup_workflow.id
}

output "cluster_setup_workflow_name" {
  description = "Name of the cluster setup workflow"
  value       = google_workflows_workflow.ibm_byoc_netezza_cluster_setup_workflow.name
}

output "delete_cluster_setup_workflow_id" {
  description = "ID of the delete cluster setup workflow"
  value       = google_workflows_workflow.ibm_byoc_netezza_delete_cluster_setup_workflow.id
}

output "delete_cluster_setup_workflow_name" {
  description = "Name of the delete cluster setup workflow"
  value       = google_workflows_workflow.ibm_byoc_netezza_delete_cluster_setup_workflow.name
}

output "netezza_engine_deploy_workflow_id" {
  description = "ID of the Netezza engine deploy workflow"
  value       = google_workflows_workflow.ibm_byoc_netezza_engine_deploy_workflow.id
}

output "netezza_engine_deploy_workflow_name" {
  description = "Name of the Netezza engine deploy workflow"
  value       = google_workflows_workflow.ibm_byoc_netezza_engine_deploy_workflow.name
}

# ==============================================================================
# WORKFLOW IDS MAP
# ==============================================================================

output "workflow_ids" {
  description = "Map of all workflow IDs"
  value = {
    cluster_setup         = google_workflows_workflow.ibm_byoc_netezza_cluster_setup_workflow.id
    delete_cluster_setup  = google_workflows_workflow.ibm_byoc_netezza_delete_cluster_setup_workflow.id
    netezza_engine_deploy = google_workflows_workflow.ibm_byoc_netezza_engine_deploy_workflow.id
  }
}