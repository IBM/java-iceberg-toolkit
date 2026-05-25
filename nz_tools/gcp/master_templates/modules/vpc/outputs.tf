# ==============================================================================
# VPC Module Outputs
# ==============================================================================

# ==============================================================================
# VPC Network Outputs
# ==============================================================================

output "vpc_id" {
  description = "The ID of the VPC network"
  value       = google_compute_network.ibm_byoc_vpc.id
}

output "vpc_name" {
  description = "The name of the VPC network"
  value       = google_compute_network.ibm_byoc_vpc.name
}

output "vpc_self_link" {
  description = "The self link of the VPC network"
  value       = google_compute_network.ibm_byoc_vpc.self_link
}

# ==============================================================================
# Subnet Outputs
# ==============================================================================

output "subnets" {
  description = "Map of subnet details"
  value = {
    for k, subnet in google_compute_subnetwork.ibm_byoc_subnets : k => {
      id               = subnet.id
      name             = subnet.name
      self_link        = subnet.self_link
      ip_cidr_range    = subnet.ip_cidr_range
      gateway_address  = subnet.gateway_address
      region           = subnet.region
      secondary_ranges = subnet.secondary_ip_range
    }
  }
}

output "subnet_ids" {
  description = "List of subnet IDs"
  value       = [for subnet in google_compute_subnetwork.ibm_byoc_subnets : subnet.id]
}

output "subnet_self_links" {
  description = "List of subnet self links"
  value       = [for subnet in google_compute_subnetwork.ibm_byoc_subnets : subnet.self_link]
}

# ==============================================================================
# VPC Connector Outputs
# ==============================================================================

output "vpc_connector_id" {
  description = "The ID of the VPC Access Connector"
  value       = var.create_vpc_connector ? google_vpc_access_connector.ibm_byoc_vpc_connector[0].id : null
}

output "vpc_connector_name" {
  description = "The name of the VPC Access Connector"
  value       = var.create_vpc_connector ? google_vpc_access_connector.ibm_byoc_vpc_connector[0].name : null
}

output "vpc_connector_self_link" {
  description = "The self link of the VPC Access Connector"
  value       = var.create_vpc_connector ? google_vpc_access_connector.ibm_byoc_vpc_connector[0].self_link : null
}

output "vpc_connector_state" {
  description = "The state of the VPC Access Connector"
  value       = var.create_vpc_connector ? google_vpc_access_connector.ibm_byoc_vpc_connector[0].state : null
}