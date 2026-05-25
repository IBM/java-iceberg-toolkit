terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

resource "google_container_cluster" "ibm_byoc_gke" {
  name                = "${var.naming_prefix}-gke"
  project             = var.project_id
  location            = var.location
  network             = var.network
  subnetwork          = var.subnetwork
  description         = "IBM BYOC GKE cluster for dataplane"
  deletion_protection = var.deletion_protection

  networking_mode          = "VPC_NATIVE"
  remove_default_node_pool = false
  node_locations           = var.node_locations

  datapath_provider     = var.datapath_provider
  enable_shielded_nodes = var.enable_shielded_nodes
  resource_labels       = var.labels

  dynamic "release_channel" {
    for_each = var.release_channel != null && var.release_channel != "" ? [1] : []
    content {
      channel = var.release_channel
    }
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = var.pods_secondary_range_name
    services_secondary_range_name = var.services_secondary_range_name
  }

  private_cluster_config {
    enable_private_nodes    = var.enable_private_nodes
    enable_private_endpoint = var.enable_private_endpoint
    master_ipv4_cidr_block  = var.master_ipv4_cidr_block
  }

  # Database encryption using customer-managed encryption keys (CMEK)
  dynamic "database_encryption" {
    for_each = var.database_encryption_key_name != null ? [1] : []
    content {
      state    = "ENCRYPTED"
      key_name = var.database_encryption_key_name
    }
  }

  dynamic "master_authorized_networks_config" {
    for_each = length(var.master_authorized_networks) > 0 ? [1] : []
    content {
      dynamic "cidr_blocks" {
        for_each = var.master_authorized_networks
        content {
          cidr_block   = cidr_blocks.value.cidr_block
          display_name = cidr_blocks.value.display_name
        }
      }
    }
  }

  logging_config {
    enable_components = var.logging_components
  }

  monitoring_config {
    enable_components = var.monitoring_components

    managed_prometheus {
      enabled = var.enable_managed_prometheus
    }
  }

  # Workload Identity configuration (required for GKE metadata server)
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  addons_config {
    http_load_balancing {
      disabled = !var.enable_http_load_balancing
    }

    gce_persistent_disk_csi_driver_config {
      enabled = var.enable_gce_persistent_disk_csi_driver
    }

    gcp_filestore_csi_driver_config {
      enabled = var.enable_gcp_filestore_csi_driver
    }

    dns_cache_config {
      enabled = var.enable_dns_cache
    }

    gke_backup_agent_config {
      enabled = var.enable_gke_backup
    }
  }

  # Maintenance Policy - Define maintenance windows and exclusions
  dynamic "maintenance_policy" {
    for_each = var.enable_maintenance_policy ? [1] : []
    content {
      # Maintenance exclusions - periods when no upgrades should occur
      dynamic "maintenance_exclusion" {
        for_each = var.use_dynamic_maintenance_window ? [
          {
            name       = var.maintenance_exclusions[0].name
            start_time = timestamp()
            end_time   = timeadd(timestamp(), "${var.maintenance_exclusion_duration_days * 24}h")
            scope      = var.maintenance_exclusions[0].scope
          }
        ] : var.maintenance_exclusions
        content {
          exclusion_name = maintenance_exclusion.value.name
          start_time     = maintenance_exclusion.value.start_time
          end_time       = maintenance_exclusion.value.end_time

          exclusion_options {
            scope = maintenance_exclusion.value.scope
          }
        }
      }

      # Optional: Daily maintenance window (if specified)
      dynamic "daily_maintenance_window" {
        for_each = var.daily_maintenance_window != null ? [var.daily_maintenance_window] : []
        content {
          start_time = daily_maintenance_window.value.start_time
        }
      }

      # Optional: Recurring maintenance window (if specified)
      dynamic "recurring_window" {
        for_each = var.recurring_maintenance_window != null ? [var.recurring_maintenance_window] : []
        content {
          start_time = recurring_window.value.start_time
          end_time   = recurring_window.value.end_time
          recurrence = recurring_window.value.recurrence
        }
      }
    }
  }

  # Node Auto-Provisioning (NAP) - Automatically creates node pools based on workload requirements
  dynamic "cluster_autoscaling" {
    for_each = var.enable_node_auto_provisioning ? [1] : []
    content {
      enabled = true

      # Zones where auto-provisioned nodes can be created
      auto_provisioning_locations = var.node_auto_provisioning_locations

      # Autoscaling profile: BALANCED or OPTIMIZE_UTILIZATION
      autoscaling_profile = var.node_autoscaling_profile

      # Resource limits for auto-provisioned node pools
      resource_limits {
        resource_type = "cpu"
        minimum       = var.node_auto_provisioning_limits.cpu_min
        maximum       = var.node_auto_provisioning_limits.cpu_max
      }

      resource_limits {
        resource_type = "memory"
        minimum       = var.node_auto_provisioning_limits.memory_min
        maximum       = var.node_auto_provisioning_limits.memory_max
      }

      # Auto-provisioning defaults for new node pools
      auto_provisioning_defaults {
        service_account = var.default_node_pool.service_account
        oauth_scopes    = var.default_node_pool.oauth_scopes

        disk_size = var.node_auto_provisioning_defaults.disk_size_gb
        disk_type = var.node_auto_provisioning_defaults.disk_type

        image_type        = var.node_auto_provisioning_defaults.image_type
        min_cpu_platform  = var.node_auto_provisioning_defaults.min_cpu_platform
        boot_disk_kms_key = var.node_auto_provisioning_defaults.boot_disk_kms_key

        shielded_instance_config {
          enable_integrity_monitoring = true
          enable_secure_boot          = var.node_auto_provisioning_defaults.enable_secure_boot
        }

        management {
          auto_repair  = true
          auto_upgrade = false
        }

        upgrade_settings {
          max_surge       = var.node_auto_provisioning_defaults.max_surge
          max_unavailable = var.node_auto_provisioning_defaults.max_unavailable
          strategy        = var.node_auto_provisioning_defaults.upgrade_strategy
        }
      }
    }
  }

  node_pool {
    name               = var.default_node_pool.name
    node_locations     = var.default_node_pool.node_locations
    initial_node_count = var.default_node_pool.initial_node_count
    max_pods_per_node  = var.default_node_pool.max_pods_per_node
    version            = var.default_node_pool.version

    management {
      auto_repair  = var.default_node_pool.auto_repair
      auto_upgrade = var.default_node_pool.auto_upgrade
    }

    autoscaling {
      min_node_count = var.default_node_pool.min_node_count
      max_node_count = var.default_node_pool.max_node_count
    }

    node_config {
      service_account = var.default_node_pool.service_account
      machine_type    = var.default_node_pool.machine_type
      disk_size_gb    = var.default_node_pool.disk_size_gb
      disk_type       = var.default_node_pool.disk_type
      image_type      = var.default_node_pool.image_type
      oauth_scopes    = var.default_node_pool.oauth_scopes
      tags            = var.default_node_pool.tags
      spot            = var.default_node_pool.spot
      labels          = var.labels

      # Boot disk encryption with customer-managed encryption key (CMEK)
      boot_disk_kms_key = var.default_node_pool.boot_disk_kms_key

      metadata = {
        disable-legacy-endpoints = "true"
      }

      # GKE metadata server configuration (requires Workload Identity)
      workload_metadata_config {
        mode = "GKE_METADATA"
      }

      shielded_instance_config {
        enable_integrity_monitoring = true
        enable_secure_boot          = var.default_node_pool.enable_secure_boot
      }
    }

    network_config {
      enable_private_nodes = var.enable_private_nodes
      create_pod_range     = false
      pod_range            = var.pods_secondary_range_name
    }
  }
}

