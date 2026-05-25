# ==============================================================================
# GCP Artifact Registry Module for BYOC Netezza
# ==============================================================================
# Purpose: Store Netezza deployment container images
# Security: Private registry with ops service account access only
# Deployment: Dataplane-only (no control plane components)
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
# Artifact Registry Repository
# ==============================================================================

resource "google_artifact_registry_repository" "ibm_byoc_netezza_registry" {
  location      = var.region
  repository_id = "${var.naming_prefix}-registry-netezza"
  description   = "Private container registry for Netezza deployment images - Dataplane only"
  format        = "DOCKER"
  project       = var.project_id

  # Dataplane security: Private mode
  mode = "STANDARD_REPOSITORY"

  # Docker configuration
  docker_config {
    immutable_tags = var.enable_immutable_tags
  }

  labels = var.labels

  # Cleanup policies for image lifecycle management
  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"

    condition {
      tag_state  = "UNTAGGED"
      older_than = "${var.untagged_image_retention_days * 86400}s"
    }
  }

  cleanup_policies {
    id     = "keep-recent-versions"
    action = "KEEP"

    most_recent_versions {
      keep_count = var.keep_recent_versions
    }
  }
}

