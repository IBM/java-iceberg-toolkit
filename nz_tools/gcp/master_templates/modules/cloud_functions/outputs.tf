# ==============================================================================
# Cloud Functions Module Outputs
# ==============================================================================

# ==============================================================================
# FUNCTION URLs
# ==============================================================================

output "send_dataplane_status_url" {
  description = "HTTPS URL for send_dataplane_status Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_send_dataplane_status_function.url
}

output "kube_api_proxy_url" {
  description = "HTTPS URL for kube_api_proxy Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_kube_api_proxy_function.url
}

output "install_operator_url" {
  description = "HTTPS URL for install_operator Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_install_operator_function.url
}

output "get_operator_status_url" {
  description = "HTTPS URL for get_operator_status Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_get_operator_status_function.url
}

output "common_serverless_url" {
  description = "HTTPS URL for common_serverless Cloud Function (orchestrator)"
  value       = google_cloudfunctions2_function.ibm_byoc_common_serverless_function.url
}

# ==============================================================================
# FUNCTION NAMES
# ==============================================================================

output "send_dataplane_status_name" {
  description = "Name of send_dataplane_status Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_send_dataplane_status_function.name
}

output "kube_api_proxy_name" {
  description = "Name of kube_api_proxy Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_kube_api_proxy_function.name
}

output "install_operator_name" {
  description = "Name of install_operator Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_install_operator_function.name
}

output "get_operator_status_name" {
  description = "Name of get_operator_status Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_get_operator_status_function.name
}

output "common_serverless_name" {
  description = "Name of common_serverless Cloud Function (orchestrator)"
  value       = google_cloudfunctions2_function.ibm_byoc_common_serverless_function.name
}

# ==============================================================================
# FUNCTION IDS
# ==============================================================================

output "send_dataplane_status_id" {
  description = "ID of send_dataplane_status Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_send_dataplane_status_function.id
}

output "kube_api_proxy_id" {
  description = "ID of kube_api_proxy Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_kube_api_proxy_function.id
}

output "install_operator_id" {
  description = "ID of install_operator Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_install_operator_function.id
}

output "get_operator_status_id" {
  description = "ID of get_operator_status Cloud Function"
  value       = google_cloudfunctions2_function.ibm_byoc_get_operator_status_function.id
}

output "common_serverless_id" {
  description = "ID of common_serverless Cloud Function (orchestrator)"
  value       = google_cloudfunctions2_function.ibm_byoc_common_serverless_function.id
}

# ==============================================================================
# COMBINED OUTPUTS FOR WORKFLOWS
# ==============================================================================

output "function_urls" {
  description = "Map of all Cloud Function URLs for use in Workflows"
  value = {
    send_dataplane_status = google_cloudfunctions2_function.ibm_byoc_send_dataplane_status_function.url
    kube_api_proxy        = google_cloudfunctions2_function.ibm_byoc_kube_api_proxy_function.url
    install_operator      = google_cloudfunctions2_function.ibm_byoc_install_operator_function.url
    get_operator_status   = google_cloudfunctions2_function.ibm_byoc_get_operator_status_function.url
    common_serverless     = google_cloudfunctions2_function.ibm_byoc_common_serverless_function.url
  }
}

output "function_names" {
  description = "Map of all Cloud Function names"
  value = {
    send_dataplane_status = google_cloudfunctions2_function.ibm_byoc_send_dataplane_status_function.name
    kube_api_proxy        = google_cloudfunctions2_function.ibm_byoc_kube_api_proxy_function.name
    install_operator      = google_cloudfunctions2_function.ibm_byoc_install_operator_function.name
    get_operator_status   = google_cloudfunctions2_function.ibm_byoc_get_operator_status_function.name
    common_serverless     = google_cloudfunctions2_function.ibm_byoc_common_serverless_function.name
  }
}