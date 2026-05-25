# ==============================================================================
# ==============================================================================
# VPC Module for BYOC - Based on test-vpc configuration
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
# VPC Network
# ==============================================================================
# NOTE: This resource should be imported if VPC already exists
# Use: terraform import module.vpc.google_compute_network.ibm_byoc_vpc projects/PROJECT_ID/global/networks/VPC_NAME

resource "google_compute_network" "ibm_byoc_vpc" {
  name                            = "${var.naming_prefix}-vpc"
  project                         = var.project_id
  auto_create_subnetworks         = false
  routing_mode                    = var.routing_mode
  mtu                             = var.mtu
  delete_default_routes_on_create = var.delete_default_routes_on_create
  description                     = var.vpc_description

  lifecycle {
    # prevent_destroy = true
    # Ignore changes to name to allow importing existing VPC with different name
    ignore_changes = [name]
  }
}

# ==============================================================================
# VPC Access Connector (for Cloud Functions)
# ==============================================================================

resource "google_vpc_access_connector" "ibm_byoc_vpc_connector" {
  count = var.create_vpc_connector ? 1 : 0
  # VPC connector name must match pattern: ^[a-z][-a-z0-9]{0,23}[a-z0-9]$ (max 25 chars)
  # Generate compliant name from dataplane_id by removing hyphens and prefixing with 'vpc'
  # Example: "dp-776600" -> "vpcdp776600" (11 chars, valid)
  name          = coalesce(var.vpc_connector_name, lower("vpc${replace(var.dataplane_id, "-", "")}"))
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.ibm_byoc_vpc.name
  ip_cidr_range = var.vpc_connector_ip_cidr_range

  min_instances = var.vpc_connector_min_instances
  max_instances = var.vpc_connector_max_instances
  machine_type  = var.vpc_connector_machine_type
}

# ==============================================================================
# Cloud Router (Required for Cloud NAT)
# ==============================================================================

resource "google_compute_router" "ibm_byoc_router" {
  count   = var.create_nat_gateway ? 1 : 0
  name    = "${var.naming_prefix}-vpc-router"
  project = var.project_id
  region  = var.region
  network = google_compute_network.ibm_byoc_vpc.id

  bgp {
    asn = var.router_asn
  }
}

# ==============================================================================
# Cloud NAT (Required for private GKE nodes to access internet)
# ==============================================================================

resource "google_compute_router_nat" "ibm_byoc_nat" {
  count   = var.create_nat_gateway ? 1 : 0
  name    = "${var.naming_prefix}-vpc-nat"
  project = var.project_id
  region  = var.region
  router  = google_compute_router.ibm_byoc_router[0].name

  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = var.nat_log_config.enable
    filter = var.nat_log_config.filter
  }
}