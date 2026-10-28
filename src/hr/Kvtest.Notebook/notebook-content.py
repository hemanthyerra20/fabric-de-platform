# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
key_vault_uri = "https://az1-prod-eaglei-dev-kv.vault.azure.net"
secret_name = "eaglei-workday-client"

# Access the secret
secret_value = notebookutils.credentials.getSecret(key_vault_uri, secret_name)
print(secret_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
