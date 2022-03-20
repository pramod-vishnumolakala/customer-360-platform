# ============================================================
# Customer 360 Platform — Terraform Infrastructure (Azure)
# Author: Pramod Vishnumolakala
# ============================================================

terraform {
  required_version = ">= 1.4"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
  }
  backend "azurerm" {
    resource_group_name  = "pramod-terraform-rg"
    storage_account_name = "pramodtfstate"
    container_name       = "tfstate"
    key                  = "customer360/terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

variable "environment"  { default = "production" }
variable "location"     { default = "East US" }
variable "project_name" { default = "customer360" }

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    Owner       = "pramod.vishnumolakala"
  }
}


# ── Resource Group ────────────────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "pramod-${var.project_name}-rg"
  location = var.location
  tags     = local.tags
}


# ── Key Vault ─────────────────────────────────────────────────────────
resource "azurerm_key_vault" "main" {
  name                = "pramod-c360-kv"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "premium"
  tags                = local.tags

  purge_protection_enabled   = true
  soft_delete_retention_days = 90
}

data "azurerm_client_config" "current" {}


# ── ADLS Gen2 ─────────────────────────────────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = "pramodc360adls"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # hierarchical namespace = ADLS Gen2
  tags                     = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "customer360" {
  name               = "customer360"
  storage_account_id = azurerm_storage_account.adls.id
}

# Medallion layers
resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.customer360.name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver" {
  path               = "silver"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.customer360.name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold" {
  path               = "gold"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.customer360.name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}


# ── Azure Databricks ──────────────────────────────────────────────────
resource "azurerm_databricks_workspace" "main" {
  name                = "pramod-c360-databricks"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"
  tags                = local.tags
}


# ── Azure Data Factory ────────────────────────────────────────────────
resource "azurerm_data_factory" "main" {
  name                = "pramod-c360-adf"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}


# ── Azure Synapse Analytics ───────────────────────────────────────────
resource "azurerm_synapse_workspace" "main" {
  name                                 = "pramod-c360-synapse"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.customer360.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.synapse_password
  tags                                 = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_synapse_sql_pool" "main" {
  name                 = "customer360pool"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  sku_name             = "DW300c"
  create_mode          = "Default"
  tags                 = local.tags
}


# ── Azure Purview ─────────────────────────────────────────────────────
resource "azurerm_purview_account" "main" {
  name                = "pramod-c360-purview"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

variable "synapse_password" {
  description = "Synapse SQL admin password"
  sensitive   = true
}


# ── Outputs ───────────────────────────────────────────────────────────
output "adls_account_name"      { value = azurerm_storage_account.adls.name }
output "databricks_workspace"   { value = azurerm_databricks_workspace.main.workspace_url }
output "synapse_workspace"      { value = azurerm_synapse_workspace.main.connectivity_endpoints }
output "adf_name"               { value = azurerm_data_factory.main.name }
output "purview_account"        { value = azurerm_purview_account.main.name }
