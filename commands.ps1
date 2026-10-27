# Set DATABRICKS_CONFIG_PROFILE to your Databricks CLI profile name before running.
$profile = $env:DATABRICKS_CONFIG_PROFILE
if ([string]::IsNullOrWhiteSpace($profile)) {
    throw "Set DATABRICKS_CONFIG_PROFILE to your Databricks CLI profile name before running this script."
}

# DEV
databricks bundle validate -t dev -p $profile
databricks bundle deploy -t dev -p $profile
databricks bundle run finops_accelerator_job -t dev -p $profile

# PROD
databricks bundle validate -t prod -p $profile
databricks bundle deploy -t prod -p $profile
databricks bundle run finops_accelerator_job -t prod -p $profile
