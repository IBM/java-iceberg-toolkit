# ==============================================================================
# Subnets Configuration
# ==============================================================================
# NOTE: Subnets should be imported if they already exist
# Use: terraform import module.vpc.google_compute_subnetwork.ibm_byoc_subnets[\"primary\"] projects/PROJECT_ID/regions/REGION/subnetworks/SUBNET_NAME

resource "google_compute_subnetwork" "ibm_byoc_subnets" {
  for_each = var.subnets

  name                     = each.key == "primary" ? "${var.naming_prefix}-subnet" : "${var.naming_prefix}-${each.key}-subnet"
  project                  = var.project_id
  region                   = each.value.region
  network                  = google_compute_network.ibm_byoc_vpc.id
  ip_cidr_range            = each.value.ip_cidr_range
  private_ip_google_access = each.value.private_ip_google_access
  description              = each.value.description

  # Secondary IP ranges for GKE pods and services
  dynamic "secondary_ip_range" {
    for_each = each.value.secondary_ip_ranges
    content {
      range_name    = secondary_ip_range.value.range_name
      ip_cidr_range = secondary_ip_range.value.ip_cidr_range
    }
  }

  # Flow logs configuration
  dynamic "log_config" {
    for_each = each.value.enable_flow_logs ? [1] : []
    content {
      aggregation_interval = each.value.flow_logs_config.aggregation_interval
      flow_sampling        = each.value.flow_logs_config.flow_sampling
      metadata             = each.value.flow_logs_config.metadata
    }
  }

  lifecycle {
    # prevent_destroy = true
    # Ignore changes to name to allow importing existing subnet with different name
    ignore_changes = [name]
  }
}