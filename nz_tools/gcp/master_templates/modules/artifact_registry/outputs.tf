# ==============================================================================
# Artifact Registry Module Outputs
# ==============================================================================

output "repository_id" {
  description = "ID of the Artifact Registry repository"
  value       = google_artifact_registry_repository.ibm_byoc_netezza_registry.id
}

output "repository_name" {
  description = "Name of the Artifact Registry repository"
  value       = google_artifact_registry_repository.ibm_byoc_netezza_registry.name
}

output "repository_url" {
  description = "URL of the Artifact Registry repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ibm_byoc_netezza_registry.repository_id}"
}

output "repository_location" {
  description = "Location of the Artifact Registry repository"
  value       = google_artifact_registry_repository.ibm_byoc_netezza_registry.location
}

output "repository_format" {
  description = "Format of the Artifact Registry repository"
  value       = google_artifact_registry_repository.ibm_byoc_netezza_registry.format
}

# For use in environment variables
output "registry_endpoint" {
  description = "Docker registry endpoint for image push/pull operations"
  value       = "${var.region}-docker.pkg.dev"
}

output "full_repository_path" {
  description = "Full repository path for Docker operations"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.ibm_byoc_netezza_registry.repository_id}"
}