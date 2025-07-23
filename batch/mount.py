# Define variables
container_name = "nyc-taxi"  # or create it if not yet created
storage_account_name = "lamdadatalake"
sas_token = r"?sp=racwdl&st=2025-07-22T23:13:04Z&se=2025-07-23T07:28:04Z&spr=https&sv=2024-11-04&sr=c&sig=CKbO%2BL71zv%2Fp1Dep0QnEsFDqzuBZT1f25RW1eG174fQ%3D"

# Mount the container
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = f"/mnt/nyc-taxi",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)
