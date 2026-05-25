output "cluster_id" {
  description = "ID of the GKE cluster"
  value       = google_container_cluster.ibm_byoc_gke.id
}

output "cluster_name" {
  description = "Name of the GKE cluster"
  value       = google_container_cluster.ibm_byoc_gke.name
}

output "cluster_location" {
  description = "Location of the GKE cluster"
  value       = google_container_cluster.ibm_byoc_gke.location
}

output "cluster_endpoint" {
  description = "Endpoint of the GKE cluster"
  value       = google_container_cluster.ibm_byoc_gke.endpoint
}

output "cluster_ca_certificate" {
  description = "Base64 encoded public CA certificate for the cluster"
  value       = google_container_cluster.ibm_byoc_gke.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "default_node_pool_name" {
  description = "Name of the default/infra node pool"
  value       = google_container_cluster.ibm_byoc_gke.node_pool[0].name
}

output "gke_summary" {
  description = "Summary of the GKE deployment"
  value = {
    cluster_name           = google_container_cluster.ibm_byoc_gke.name
    cluster_location       = google_container_cluster.ibm_byoc_gke.location
    network                = google_container_cluster.ibm_byoc_gke.network
    subnetwork             = google_container_cluster.ibm_byoc_gke.subnetwork
    default_node_pool_name = google_container_cluster.ibm_byoc_gke.node_pool[0].name
  }
}