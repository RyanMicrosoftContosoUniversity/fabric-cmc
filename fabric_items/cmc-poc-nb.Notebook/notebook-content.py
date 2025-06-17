# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Central Management Console Back End
# Back End Connectivity for Central Management Console

# PARAMETERS CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

workspace_id = 'a046cf0f-8dca-4b61-b95e-7adf68fb4b0a'
dataset_id = '708da792-a344-4079-b205-61c587a51600'



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import os
import notebookutils
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
    """
    Function to retrieve an api token used to authenticate with Microsoft Fabric APIs

    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv

    """
    client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
    tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
    client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    scope = 'https://analysis.windows.net/powerbi/api/.default'
    token = credential.get_token(scope).token

    return token

def get_dataset_refresh_info(workspace_id:str, dataset_id:str, api_token:str)->pd.DataFrame:
    """
    https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group
    scopes required: Dataset.ReadWrite.All or Dataset.Read.All

    GET https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes

    workspace_id:str: The Workspace ID where the semantic model/dataset resides
    dataset_id:str: The Dataset ID to get refresh info for
    api_token:str: The api token to authenticate with the API

    returns:
        refresh_history_pd_df:pd.DataFrame: DataFrame of the refresh history
    """
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return pd.DataFrame(response.json()['value'])

def start_dataset_refresh(workspace_id:str, dataset_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group
    scopes required: Dataset.ReadWrite.All

    POST https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes

    workspace_id:str: The workspace ID where the semantic model/dataset resides
    dataset_id:str: The Dataset ID to refresh
    api_token:str: The api token used to authenticate with the API

    returns:
        pass
    """
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers)

    if response.status_code >=200 and response.status_code <300:
        print(f'Dataset Refresh request to workspace id:{workspace_id} and dataset id:{dataset_id} sent successfully')

    return response

def cancel_dataset_refresh(workspace_id:str, dataset_id:str, refresh_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/cancel-refresh-in-group
    scopes required: Dataset.ReadWrite.All

    DELETE https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes/{refreshId}

    workspace_id:str: The workspace ID where the semantic model/dataset resides
    dataset_id:str: The Dataset ID of the active refresh to be cancelled
    api_token:str: The api token used to authenticate with the API

    returns:
        pass
    """
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{refresh_id}'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.delete(url, headers=headers)

    if response.status_code==409:
        print(f'Dataset Refresh already in a completed state; cannot cancel')

    return response

def get_all_connections(api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connections?tabs=HTTP
    scopes: Connection.Read.All or Connection.ReadWrite.All

    GET https://api.fabric.microsoft.com/v1/connections


    """
    url = 'https://api.fabric.microsoft.com/v1/connections'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    return response


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get oauth token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Dataset/SM Refresh Info
dataset_refresh_history = get_dataset_refresh_info(workspace_id, dataset_id, token)

dataset_refresh_history

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Start Dataset/SM Refresh
# https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group

resp = start_dataset_refresh(workspace_id, dataset_id, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cancel Dataset/SM Refresh
# https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/cancel-refresh-in-group
refresh_id = '2b2abe5c-330e-436b-bcd9-8c099254bc4a'

cancel_resp = cancel_dataset_refresh(workspace_id, dataset_id, refresh_id, token)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cancel_resp.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
