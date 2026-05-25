# ==============================================================================
# GCP Cloud Functions Module for BYOC
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
# Cloud Function 1: send_dataplane_status
# ==============================================================================

resource "google_cloudfunctions2_function" "ibm_byoc_send_dataplane_status_function" {
  name        = "${var.naming_prefix}-function-send-dataplane-status"
  location    = var.region
  description = "Sends dataplane status updates to Pub/Sub topic"
  project     = var.project_id
  labels      = var.labels

  build_config {
    runtime     = var.python_runtime
    entry_point = "main"

    source {
      storage_source {
        bucket = var.function_source_bucket
        object = var.send_dataplane_status_source_object
      }
    }
  }

  service_config {
    max_instance_count             = var.max_instance_count
    min_instance_count             = var.min_instance_count
    available_memory               = var.memory_mb
    timeout_seconds                = var.timeout_seconds
    service_account_email          = var.ops_sa_email # Ops SA for runtime execution
    vpc_connector                  = var.vpc_connector
    vpc_connector_egress_settings  = var.vpc_connector_egress_settings
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true

    environment_variables = merge(
      var.common_env_vars,
      var.send_dataplane_status_env_vars
    )
  }
}

# ==============================================================================
# Cloud Function 2: kube_api_proxy
# ==============================================================================

resource "google_cloudfunctions2_function" "ibm_byoc_kube_api_proxy_function" {
  name        = "${var.naming_prefix}-function-kube-api-proxy"
  location    = var.region
  description = "Proxies Kubernetes API calls"
  project     = var.project_id
  labels      = var.labels

  build_config {
    runtime     = var.python_runtime
    entry_point = "main"

    source {
      storage_source {
        bucket = var.function_source_bucket
        object = var.kube_api_proxy_source_object
      }
    }
  }

  service_config {
    max_instance_count             = var.max_instance_count
    min_instance_count             = var.min_instance_count
    available_memory               = var.memory_mb
    timeout_seconds                = var.timeout_seconds
    service_account_email          = var.ops_sa_email # Ops SA for runtime execution
    vpc_connector                  = var.vpc_connector
    vpc_connector_egress_settings  = var.vpc_connector_egress_settings
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true

    environment_variables = merge(
      var.common_env_vars,
      var.kube_api_proxy_env_vars
    )
  }
}

# ==============================================================================
# Cloud Function 3: install_operator
# ==============================================================================

resource "google_cloudfunctions2_function" "ibm_byoc_install_operator_function" {
  name        = "${var.naming_prefix}-function-install-operator"
  location    = var.region
  description = "Installs cluster operators"
  project     = var.project_id
  labels      = var.labels

  build_config {
    runtime     = var.python_runtime
    entry_point = "main"

    source {
      storage_source {
        bucket = var.function_source_bucket
        object = var.install_operator_source_object
      }
    }
  }

  service_config {
    max_instance_count             = var.max_instance_count
    min_instance_count             = var.min_instance_count
    available_memory               = var.memory_mb
    timeout_seconds                = var.timeout_seconds
    service_account_email          = var.ops_sa_email # Ops SA for runtime execution
    vpc_connector                  = var.vpc_connector
    vpc_connector_egress_settings  = var.vpc_connector_egress_settings
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true

    environment_variables = merge(
      var.common_env_vars,
      var.install_operator_env_vars
    )
  }
}

# ==============================================================================
# Cloud Function 4: get_operator_status
# ==============================================================================

resource "google_cloudfunctions2_function" "ibm_byoc_get_operator_status_function" {
  name        = "${var.naming_prefix}-function-get-operator-status"
  location    = var.region
  description = "Checks operator installation status"
  project     = var.project_id
  labels      = var.labels

  build_config {
    runtime     = var.python_runtime
    entry_point = "main"

    source {
      storage_source {
        bucket = var.function_source_bucket
        object = var.get_operator_status_source_object
      }
    }
  }

  service_config {
    max_instance_count             = var.max_instance_count
    min_instance_count             = var.min_instance_count
    available_memory               = var.memory_mb
    timeout_seconds                = var.timeout_seconds
    service_account_email          = var.ops_sa_email # Ops SA for runtime execution
    vpc_connector                  = var.vpc_connector
    vpc_connector_egress_settings  = var.vpc_connector_egress_settings
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true

    environment_variables = merge(
      var.common_env_vars,
      var.get_operator_status_env_vars
    )
  }
}

# ==============================================================================
# Cloud Function 5: common_serverless (CSF) - Orchestrator
# Triggered by Pub/Sub input queue
# ==============================================================================

resource "google_cloudfunctions2_function" "ibm_byoc_common_serverless_function" {
  name        = "${var.naming_prefix}-function-common-serverless"
  location    = var.region
  description = "Orchestrator function - triggered by Pub/Sub input queue"
  project     = var.project_id
  labels      = var.labels

  build_config {
    runtime     = var.python_runtime
    entry_point = "main"

    source {
      storage_source {
        bucket = var.function_source_bucket
        object = var.common_serverless_source_object
      }
    }
  }

  service_config {
    max_instance_count             = var.max_instance_count
    min_instance_count             = var.min_instance_count
    available_memory               = var.memory_mb
    timeout_seconds                = var.timeout_seconds
    service_account_email          = var.ops_sa_email # Ops SA for runtime execution
    vpc_connector                  = var.vpc_connector
    vpc_connector_egress_settings  = var.vpc_connector_egress_settings
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true

    environment_variables = merge(
      var.common_env_vars,
      var.common_serverless_env_vars
    )
  }

  # Eventarc trigger for Pub/Sub input queue
  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = var.input_queue_topic_id
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = var.pubsub_invoker_service_account
  }
}

