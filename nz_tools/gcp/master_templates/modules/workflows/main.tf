# ==============================================================================
# GCP Workflows Module 
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
# Data Sources - Fetch Workflow Definitions from GCS
# ==============================================================================

data "google_storage_bucket_object_content" "cluster_setup_workflow" {
  name   = var.cluster_setup_workflow_file
  bucket = var.workflow_definitions_bucket
}

data "google_storage_bucket_object_content" "delete_cluster_setup_workflow" {
  name   = var.delete_cluster_setup_workflow_file
  bucket = var.workflow_definitions_bucket
}

data "google_storage_bucket_object_content" "netezza_engine_deploy_workflow" {
  name   = var.netezza_engine_deploy_workflow_file
  bucket = var.workflow_definitions_bucket
}


# ==============================================================================
# Workflow 1: Cluster Setup Workflow
# ==============================================================================

resource "google_workflows_workflow" "ibm_byoc_netezza_cluster_setup_workflow" {
  name            = "${var.naming_prefix}-workflow-cluster-setup"
  description     = "Workflow for Netezza cluster setup"
  project         = var.project_id
  region          = var.region
  service_account = var.ops_sa_email # Ops SA for runtime execution

  # Only set crypto_key_name if provided and not global (workflows require regional keys)
  crypto_key_name = var.kms_key_name != null && !can(regex("/locations/global/", var.kms_key_name)) ? var.kms_key_name : null

  source_contents = data.google_storage_bucket_object_content.cluster_setup_workflow.content

  call_log_level = "LOG_ALL_CALLS"
  labels         = var.labels
}

# ==============================================================================
# Workflow 2: Delete Cluster Setup Workflow
# ==============================================================================

resource "google_workflows_workflow" "ibm_byoc_netezza_delete_cluster_setup_workflow" {
  name            = "${var.naming_prefix}-workflow-delete-cluster-setup"
  description     = "Workflow to delete Netezza cluster setup"
  project         = var.project_id
  region          = var.region
  service_account = var.ops_sa_email # Ops SA for runtime execution

  # Only set crypto_key_name if provided and not global (workflows require regional keys)
  crypto_key_name = var.kms_key_name != null && !can(regex("/locations/global/", var.kms_key_name)) ? var.kms_key_name : null

  source_contents = data.google_storage_bucket_object_content.delete_cluster_setup_workflow.content

  call_log_level = "LOG_ALL_CALLS"
  labels         = var.labels
}

# ==============================================================================
# Workflow 3: Netezza Engine Deploy Workflow
# ==============================================================================

resource "google_workflows_workflow" "ibm_byoc_netezza_engine_deploy_workflow" {
  name            = "${var.naming_prefix}-workflow-netezza-engine-deploy"
  description     = "Workflow to deploy Netezza engine"
  project         = var.project_id
  region          = var.region
  service_account = var.ops_sa_email # Ops SA for runtime execution

  # Only set crypto_key_name if provided and not global (workflows require regional keys)
  crypto_key_name = var.kms_key_name != null && !can(regex("/locations/global/", var.kms_key_name)) ? var.kms_key_name : null

  source_contents = data.google_storage_bucket_object_content.netezza_engine_deploy_workflow.content

  call_log_level = "LOG_ALL_CALLS"
  labels         = var.labels
}