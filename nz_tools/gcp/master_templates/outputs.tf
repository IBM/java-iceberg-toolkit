# ==============================================================================
# Root Module Outputs - GCP Netezza BYOC Infrastructure
# ==============================================================================

# ==============================================================================
# GKE OUTPUTS
# ==============================================================================

output "gke_cluster_id" {
  description = "ID of the GKE cluster"
  value       = module.gke.cluster_id
}

output "gke_cluster_name" {
  description = "Name of the GKE cluster"
  value       = module.gke.cluster_name
}

output "gke_cluster_location" {
  description = "Location of the GKE cluster"
  value       = module.gke.cluster_location
}

output "gke_cluster_endpoint" {
  description = "Endpoint of the GKE cluster"
  value       = module.gke.cluster_endpoint
}

output "gke_cluster_ca_certificate" {
  description = "CA certificate of the GKE cluster"
  value       = module.gke.cluster_ca_certificate
  sensitive   = true
}

output "gke_default_node_pool_name" {
  description = "Default/infra node pool name"
  value       = module.gke.default_node_pool_name
}

output "gke_summary" {
  description = "Summary of GKE deployment"
  value       = module.gke.gke_summary
}


# ==============================================================================
# PUB/SUB OUTPUTS
# ==============================================================================

output "input_queue_topic_id" {
  description = "ID of the input queue Pub/Sub topic"
  value       = module.pubsub.input_queue_topic_id
}

output "input_queue_topic_name" {
  description = "Name of the input queue Pub/Sub topic"
  value       = module.pubsub.input_queue_topic_name
}

output "dataplane_status_topic_id" {
  description = "ID of the dataplane status Pub/Sub topic"
  value       = module.pubsub.dataplane_status_topic_id
}

output "dataplane_status_topic_name" {
  description = "Name of the dataplane status Pub/Sub topic"
  value       = module.pubsub.dataplane_status_topic_name
}

output "dataplane_status_subscription_id" {
  description = "ID of the dataplane status subscription"
  value       = module.pubsub.dataplane_status_subscription_id
}

output "input_queue_dlq_topic_id" {
  description = "ID of the input queue dead letter queue topic"
  value       = module.pubsub.input_queue_dlq_topic_id
}

output "status_dlq_topic_id" {
  description = "ID of the status dead letter queue topic"
  value       = module.pubsub.dataplane_status_dlq_topic_id
}

# ==============================================================================
# ARTIFACT REGISTRY OUTPUTS
# ==============================================================================

output "artifact_registry_repository_url" {
  description = "URL of the Artifact Registry repository for Netezza images"
  value       = module.artifact_registry.repository_url
}

output "artifact_registry_repository_name" {
  description = "Name of the Artifact Registry repository"
  value       = module.artifact_registry.repository_name
}

output "artifact_registry_endpoint" {
  description = "Docker registry endpoint for image operations"
  value       = module.artifact_registry.registry_endpoint
}

output "artifact_registry_full_path" {
  description = "Full repository path for Docker push/pull operations"
  value       = module.artifact_registry.full_repository_path
}

# ==============================================================================
# CLOUD FUNCTIONS OUTPUTS
# ==============================================================================

output "cloud_function_urls" {
  description = "Map of all Cloud Function URLs"
  value       = module.cloud_functions.function_urls
}

output "send_dataplane_status_url" {
  description = "URL for send_dataplane_status Cloud Function"
  value       = module.cloud_functions.send_dataplane_status_url
}

output "kube_api_proxy_url" {
  description = "URL for kube_api_proxy Cloud Function"
  value       = module.cloud_functions.kube_api_proxy_url
}

output "install_operator_url" {
  description = "URL for install_operator Cloud Function"
  value       = module.cloud_functions.install_operator_url
}

output "get_operator_status_url" {
  description = "URL for get_operator_status Cloud Function"
  value       = module.cloud_functions.get_operator_status_url
}
output "common_serverless_url" {
  description = "URL for common_serverless Cloud Function (CSF Orchestrator)"
  value       = module.cloud_functions.common_serverless_url
}

output "common_serverless_name" {
  description = "Name of the common_serverless Cloud Function"
  value       = module.cloud_functions.common_serverless_name
}


# ==============================================================================
# WORKFLOWS OUTPUTS
# ==============================================================================

output "workflow_ids" {
  description = "Map of all Workflow IDs"
  value       = module.workflows.workflow_ids
}

output "cluster_setup_workflow_id" {
  description = "ID of cluster setup workflow"
  value       = module.workflows.cluster_setup_workflow_id
}

output "delete_cluster_setup_workflow_id" {
  description = "ID of delete cluster setup workflow"
  value       = module.workflows.delete_cluster_setup_workflow_id
}

output "netezza_engine_deploy_workflow_id" {
  description = "ID of Netezza engine deploy workflow"
  value       = module.workflows.netezza_engine_deploy_workflow_id
}

# ==============================================================================
# DEPLOYMENT SUMMARY
# ==============================================================================

output "deployment_summary" {
  description = "Summary of deployed resources"
  value = {
    project_id    = var.project_id
    region        = var.region
    dataplane_id  = var.dataplane_id
    dataplane_crn = var.dataplane_crn

    artifact_registry = {
      repository_name = module.artifact_registry.repository_name
      repository_url  = module.artifact_registry.repository_url
    }

    pubsub = {
      input_queue_topic = module.pubsub.input_queue_topic_name
      status_topic      = module.pubsub.dataplane_status_topic_name
      dlq_topics_count  = 2
    }

    cloud_functions = {
      count = 5
      names = module.cloud_functions.function_names
    }

    workflows = {
      count                 = 3
      cluster_setup         = module.workflows.cluster_setup_workflow_name
      delete_cluster_setup  = module.workflows.delete_cluster_setup_workflow_name
      netezza_engine_deploy = module.workflows.netezza_engine_deploy_workflow_name
    }

    deployment_order = [
      "1. VPC",
      "2. Artifact Registry",
      "3. Cloud Functions",
      "4. Pub/Sub Topics and Subscriptions",
      "5. Workflows"
    ]
  }
}

# ==============================================================================
# INTEGRATION ENDPOINTS
# ==============================================================================

output "control_plane_integration" {
  description = "Endpoints for control plane integration"
  value = {
    # Control plane publishes messages to this topic
    input_queue_topic = module.pubsub.input_queue_topic_name

    # Control plane subscribes to this topic for status updates
    status_topic = module.pubsub.dataplane_status_topic_name

    # Workflows that can be triggered
    workflows = {
      cluster_setup         = module.workflows.cluster_setup_workflow_id
      delete_cluster_setup  = module.workflows.delete_cluster_setup_workflow_id
      netezza_engine_deploy = module.workflows.netezza_engine_deploy_workflow_id
    }
  }
  sensitive = false
}