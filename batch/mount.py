# Define storage parameters
container_name = "lambda"
storage_account_name = "lambdlake"
sas_token = r"sp=r&st=2025-07-24T22:43:44Z&se=2025-07-25T06:58:44Z&spr=https&sv=2024-11-04&sr=c&sig=%2FNyYRM7BA17FlghkfYsnXqz5ddSLByfF%2F4moLtR5oO8%3D"

# Define Databricks mount path
mount_point = "/mnt/lambda"

# Perform the mount
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = mount_point,
  extra_configs = {
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token
  }
)

print("Mounted 'lambda' container from 'lambdlake' to /mnt/lambda")
