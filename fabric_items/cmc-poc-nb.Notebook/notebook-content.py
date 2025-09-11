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
# Currently connecting to this semantic model: https://app.fabric.microsoft.com/groups/a046cf0f-8dca-4b61-b95e-7adf68fb4b0a/datasets/708da792-a344-4079-b205-61c587a51600/details?experience=power-bi
# 
# ### Updates
# - Get all Workspaces in Tenant


# PARAMETERS CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

workspace_id = 'a046cf0f-8dca-4b61-b95e-7adf68fb4b0a'
dataset_id = '708da792-a344-4079-b205-61c587a51600'

url = f'https://app.fabric.microsoft.com/groups/{workspace_id}/datasets/{dataset_id}/details?experience=power-bi'

lakehouse_tables_abfs_path = 'abfss://9d04a637-9cd3-41f2-8c4d-e3e1e18041a5@onelake.dfs.fabric.microsoft.com/ac4609b3-6e39-4b3a-9406-4f1d7941e8bd/Tables'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import asyncio
import aiohttp
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import os
import notebookutils
import pandas as pd
import json
from typing import Optional, Dict, Any
import logging
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, IntegerType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def admin_list_all_workspaces(capacity_id:str, api_token:str) ->dict:
    """
    List all workspaces in Fabric/PBI

    capacity_id:str: The ID of the capacity

    returns:json

    https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP
    GET https://api.fabric.microsoft.com/v1/admin/workspaces?type={type}&capacityId={capacityId}&name={name}&state={state}&continuationToken={continuationToken}

    """
    url = f'https://api.fabric.microsoft.com/v1/admin/workspaces?type=workspace&capacityId={capacity_id}&state=active'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    if response.status_code >=200 and response.status_code <300:
        print(f'ERROR: message: {response.json()}')

    return response

def _check_group_user_access_right(group_user_access_right:str):
    """
    This is used to validate the group_user_access_right value for adding user to workspace is valid
    """
    if group_user_access_right not in ('None', 'Member', 'Admin', 'Contributor', 'Viewer'):
        raise ValueError(f'Invalid group_user_access_right.  Value must be one of: None, Member, Admin, Contributor, Viewer.  Value received: {group_user_access_right} ')

def _check_principal_type(principal_type:str):
    """
    This is used to validate the principal_type value for adding user to workspace is valid

    """
    if principal_type not in ('None', 'Group', 'App'):
        raise ValueError(f'Invalid principal_type.  Value must be one of None, User, Group, App.  Value received: {principal_type}')

def add_user_to_workspace(identifier:str, group_user_access_right:str, principal_type:str, workspace_list:list, api_token:str):
    """
    Add group to workspace_id.  Given a list of workspace IDs add a objectID of a group to it

    workspace_id:str: The workspace ID
    identifier:str: The object ID of the Entra Group
    group_user_access_right:str:  The accress to be granted to the ID.  Must be one of None, Member, Admin, Contributor, Viewer
    principal_type:str: The type of principal.  Must be one of: None, User, Group, App

    https://learn.microsoft.com/en-us/rest/api/power-bi/groups/add-group-user
    POST https://api.powerbi.com/v1.0/myorg/groups/{groupId}/users
    
    https://api.powerbi.com/v1.0/myorg/groups/{groupId}/users
    """
    _check_group_user_access_right(group_user_access_right)
    _check_principal_type(principal_type)

    post_body = {
            "identifier": identifier,
            "groupUserAccessRight": group_user_access_right,
            "principalType": principal_type
        }

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    
    for workspace_id in workspace_list:
        print(f'Attempting to add user group ID:{identifier} to workspace:{workspace_id}')

        url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/users'


        response = requests.post(url, headers=headers, json=post_body)
        print(response.status_code)

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

    try:
        return pd.DataFrame(response.json()['value'])
    except:
        return response
        
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

def get_all_workspaces(api_token:str)-> json:
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces/list-workspaces?tabs=HTTP
    Get all workspaces in a tenant
    Requires Scopes: Tenant.Read.All or Tenant.ReadWrite.All

    api_token:str: The API Token used to authenticate with the APIs
    """
    url = 'https://api.fabric.microsoft.com/v1/admin/workspaces'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    if response.status_code >=200 and response.status_code < 300:
        return response.json()

def get_all_datasets_in_workspace(workspace_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/semanticmodel/items/list-semantic-models?tabs=HTTP
    GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/semanticModels
    Get all semantic models in a workspace
    Requires Scopes: Workspace.Read.All or Workspace.ReadWrite.All

    workspace_id:str: The uuid of the workspace you'd like the semantic models for
    api_token:str: The API Token used to authenticate with the APIs
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.get(url, headers=headers)

    if response.status_code >=200 and response.status_code < 300:
        return response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### test
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)
capacity_id = ' AD343E36-F335-4BA3-B261-B739F7E950B0'


workspace_json = admin_list_all_workspaces(capacity_id, api_token=token)

ws_list = []
workspace_json.json()['workspaces']

for _ in workspace_json.json()['workspaces']:
    ws_list.append(_['id'])

ws_list


ws_list = []
workspace_json.json()['workspaces']

for _ in workspace_json.json()['workspaces']:
    ws_list.append(_['id'])

for ws in ws_list:
    print(ws)

### Test def add_user_to_workspace(workspace_id:str, identifier:str, group_user_access_right:str, principal_type:str, workspace_list:list, api_token:str):
add_user_to_workspace(identifier='054b0fcd-a031-4ee0-95d9-40f896a82879', group_user_access_right='Admin', principal_type='Group', workspace_list=ws_list, api_token=token)



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

# CELL ********************

workspace_json = get_all_workspaces(token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_json['workspaces']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataset_response = get_all_datasets_in_workspace('21695bc6-4aeb-41ae-bbbd-93d8858e7665', token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(dataset_response)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ASYNC API FUNCTIONS ********************

async def get_api_token_via_akv_async(kv_uri: str, client_id_secret: str, tenant_id_secret: str, client_secret_name: str) -> str:
    """
    Async function to retrieve an api token used to authenticate with Microsoft Fabric APIs
    
    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv
    """
    try:
        # Note: notebookutils.credentials.getSecret is not async, so we'll keep it synchronous
        client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
        tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
        client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        scope = 'https://analysis.windows.net/powerbi/api/.default'
        
        # Run in executor since azure.identity is not async
        loop = asyncio.get_event_loop()
        token = await loop.run_in_executor(None, lambda: credential.get_token(scope).token)
        
        return token
    except Exception as e:
        logger.error(f"Error retrieving API token: {str(e)}")
        raise

async def get_dataset_refresh_info_async(session: aiohttp.ClientSession, workspace_id: str, dataset_id: str, schema:StructType, api_token: str) -> pd.DataFrame:
    """
    Async version of get_dataset_refresh_info
    
    session: aiohttp.ClientSession for making HTTP requests
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
    
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            # return pd.DataFrame(data.get('value', []))
            return spark.createDataFrame(data, schema=schema)
    except aiohttp.ClientError as e:
        logger.error(f"Error getting dataset refresh info: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

### DEPRECATED
# async def get_dataset_refresh_info_async(session: aiohttp.ClientSession, workspace_id: str, dataset_id: str, api_token: str) -> pd.DataFrame:
#     """
#     Async version of get_dataset_refresh_info
    
#     session: aiohttp.ClientSession for making HTTP requests
#     workspace_id:str: The Workspace ID where the semantic model/dataset resides
#     dataset_id:str: The Dataset ID to get refresh info for
#     api_token:str: The api token to authenticate with the API
    
#     returns:
#         refresh_history_pd_df:pd.DataFrame: DataFrame of the refresh history
#     """
#     url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'
    
#     headers = {
#         "Authorization": f"Bearer {api_token}",
#         "Content-Type": "application/json"
#     }
    
#     try:
#         async with session.get(url, headers=headers) as response:
#             response.raise_for_status()
#             data = await response.json()
#             return pd.DataFrame(data.get('value', []))

#     except aiohttp.ClientError as e:
#         logger.error(f"Error getting dataset refresh info: {str(e)}")
#         raise
#     except Exception as e:
#         logger.error(f"Unexpected error: {str(e)}")
#         raise

async def start_dataset_refresh_async(session: aiohttp.ClientSession, workspace_id: str, dataset_id: str, api_token: str) -> Dict[str, Any]:
    """
    Async version of start_dataset_refresh
    
    session: aiohttp.ClientSession for making HTTP requests
    workspace_id:str: The workspace ID where the semantic model/dataset resides
    dataset_id:str: The Dataset ID to refresh
    api_token:str: The api token used to authenticate with the API
    
    returns:
        Dict containing response data and status
    """
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.post(url, headers=headers) as response:
            if 200 <= response.status < 300:
                logger.info(f'Dataset Refresh request to workspace id:{workspace_id} and dataset id:{dataset_id} sent successfully')
                return {
                    "status": "success",
                    "status_code": response.status,
                    "message": "Refresh started successfully"
                }
            else:
                error_text = await response.text()
                logger.error(f"Failed to start refresh: {response.status} - {error_text}")
                return {
                    "status": "error",
                    "status_code": response.status,
                    "message": error_text
                }
    except aiohttp.ClientError as e:
        logger.error(f"Error starting dataset refresh: {str(e)}")
        raise

async def cancel_dataset_refresh_async(session: aiohttp.ClientSession, workspace_id: str, dataset_id: str, refresh_id: str, api_token: str) -> Dict[str, Any]:
    """
    Async version of cancel_dataset_refresh
    
    session: aiohttp.ClientSession for making HTTP requests
    workspace_id:str: The workspace ID where the semantic model/dataset resides
    dataset_id:str: The Dataset ID of the active refresh to be cancelled
    refresh_id:str: The refresh ID to cancel
    api_token:str: The api token used to authenticate with the API
    
    returns:
        Dict containing response data and status
    """
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{refresh_id}'
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.delete(url, headers=headers) as response:
            if response.status == 409:
                message = 'Dataset Refresh already in a completed state; cannot cancel'
                logger.info(message)
                return {
                    "status": "warning",
                    "status_code": response.status,
                    "message": message
                }
            elif 200 <= response.status < 300:
                return {
                    "status": "success",
                    "status_code": response.status,
                    "message": "Refresh cancelled successfully"
                }
            else:
                error_text = await response.text()
                logger.error(f"Failed to cancel refresh: {response.status} - {error_text}")
                return {
                    "status": "error",
                    "status_code": response.status,
                    "message": error_text
                }
    except aiohttp.ClientError as e:
        logger.error(f"Error cancelling dataset refresh: {str(e)}")
        raise

async def get_all_workspaces_async(session: aiohttp.ClientSession, capacity_id:str, api_token: str) -> Optional[Dict[str, Any]]:
    """
    Async version of get_all_workspaces

    Modified: GET https://api.fabric.microsoft.com/v1/admin/workspaces?type={type}&capacityId={capacityId}&name={name}&state={state}&continuationToken={continuationToken}
    
    session: aiohttp.ClientSession for making HTTP requests
    api_token:str: The API Token used to authenticate with the APIs
    
    returns:
        Dict containing workspace data or None if error
    """
    url = f'https://api.fabric.microsoft.com/v1/admin/workspaces?type=workspace&capacityId={capacity_id}'
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logger.error(f"Error getting all workspaces: {str(e)}")
        raise

async def get_all_datasets_in_workspace_async(session: aiohttp.ClientSession, workspace_id: str, api_token: str) -> Optional[Dict[str, Any]]:
    """
    Async version of get_all_datasets_in_workspace
    
    session: aiohttp.ClientSession for making HTTP requests
    workspace_id:str: The uuid of the workspace you'd like the semantic models for
    api_token:str: The API Token used to authenticate with the APIs
    
    returns:
        Dict containing dataset data or None if error

    docs:
    https://learn.microsoft.com/en-us/rest/api/fabric/semanticmodel/items/list-semantic-models?tabs=HTTP
    """
    
    base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }

    all_items: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        params = {}
        if next_token:
            # If the API uses continuationToken; adjust if the API uses different pagination keys
            params["continuationToken"] = next_token

        logger.debug("Fetching semantic models: workspace=%s, continuationToken=%s",
                     workspace_id, next_token)
        async with session.get(base_url, headers=headers, params=params) as resp:
            text = await resp.text()  # helpful for debugging unexpected content-types
            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError as e:
                logger.error("HTTP %s getting semantic models for workspace %s. Body: %s",
                             resp.status, workspace_id, text)
                raise

            # Some endpoints return {"value":[...], "continuationToken":"..."}
            # others might return {"items":[...]} or even a bare list.
            try:
                payload = await resp.json(content_type=None)
            except Exception:
                logger.error("Non-JSON response for workspace %s: %s", workspace_id, text)
                raise

            if isinstance(payload, list):
                # Bare array case
                all_items.extend(payload)
                break

            if isinstance(payload, dict):
                items = (
                    payload.get("value")
                    or payload.get("items")
                    or payload.get("data")
                    or []
                )
                if not isinstance(items, list):
                    logger.warning("Unexpected items type for workspace %s: %s",
                                   workspace_id, type(items))
                    items = []

                all_items.extend(items)
                # Pick the right pagination key if present
                next_token = payload.get("continuationToken") or payload.get("nextToken")
                if not next_token:
                    break
            else:
                logger.warning("Unexpected payload type for workspace %s: %s",
                               workspace_id, type(payload))
                break

    logger.info("Workspace %s: fetched %d semantic models", workspace_id, len(all_items))
    return all_items


async def get_all_connections_async(session: aiohttp.ClientSession, api_token: str) -> Dict[str, Any]:
    """
    Async version of get_all_connections
    
    session: aiohttp.ClientSession for making HTTP requests
    api_token:str: The API Token used to authenticate with the APIs
    
    returns:
        Dict containing connection data
    """
    url = 'https://api.fabric.microsoft.com/v1/connections'
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logger.error(f"Error getting all connections: {str(e)}")
        raise

# BATCH OPERATIONS ********************

async def get_multiple_datasets_refresh_info_async(workspace_dataset_pairs: list, api_token: str) -> Dict[str, pd.DataFrame]:
    """
    Get refresh info for multiple datasets concurrently
    
    workspace_dataset_pairs: List of tuples containing (workspace_id, dataset_id)
    api_token: API token for authentication
    
    returns:
        Dictionary with keys as "workspace_id:dataset_id" and values as DataFrames
    """
    timeout = aiohttp.ClientTimeout(total=300, connect=60)  # 5 minute total, 1 minute connect
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)  # Connection pooling
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for workspace_id, dataset_id in workspace_dataset_pairs:
            task = get_dataset_refresh_info_async(session, workspace_id, dataset_id, api_token)
            tasks.append((f"{workspace_id}:{dataset_id}", task))
        
        results = {}
        for key, task in tasks:
            try:
                result = await task
                results[key] = result
            except Exception as e:
                logger.error(f"Failed to get refresh info for {key}: {str(e)}")
                results[key] = pd.DataFrame()  # Empty DataFrame on error
        
        return results

async def bulk_workspace_operations_async(api_token: str) -> Dict[str, Any]:
    """
    Perform multiple workspace-related operations concurrently
    
    api_token: API token for authentication
    
    returns:
        Dictionary containing results from all operations
    """
    timeout = aiohttp.ClientTimeout(total=300, connect=60)
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Run multiple operations concurrently
        tasks = {
            'workspaces': get_all_workspaces_async(session, api_token),
            'connections': get_all_connections_async(session, api_token)
        }
        
        results = {}
        for operation, task in tasks.items():
            try:
                results[operation] = await task
            except Exception as e:
                logger.error(f"Failed to execute {operation}: {str(e)}")
                results[operation] = None
        
        return results


### Non-Async Functions
def add_workspace_id_to_dataset_dict(dataset_list:list, workspace_id:str)->list:
    """
    Given a list, process the list
    - select ['value']
    - for item in dataset_list -> item['workspace_id']

    dataset_list:list: The list to be processed
    workspace_id:str: The workspace_id to be added to each dict of the list

    returns:
        list

    Example:
    {'value': [{'id': '228c82be-411b-4fa2-a6cd-a8e06cb01f63',
   'type': 'SemanticModel',
   'displayName': 'gold_warehouse',
   'description': '',
   'workspaceId': '7afc490e-115f-472c-a205-17dc6a5bee52'},
  {'id': '1d4e18fb-f830-4aff-bb12-9fe27c5c397f',
   'type': 'SemanticModel',
   'displayName': 'silver_lakehouse',
   'description': '',
   'workspaceId': '7afc490e-115f-472c-a205-17dc6a5bee52'},
  {'id': 'a9436752-1e0b-4ea1-bf4c-d95de9544d8b',
   'type': 'SemanticModel',
   'displayName': 'metadata_lh',
   'description': '',
   'workspaceId': '7afc490e-115f-472c-a205-17dc6a5bee52'},
  {'id': '2e3b9d01-adc3-4b05-96a1-2c0217b1677a',
   'type': 'SemanticModel',
   'displayName': 'bronzeWH',
   'description': '',
   'workspaceId': '7afc490e-115f-472c-a205-17dc6a5bee52'}]}
    """
    dataset_list = dataset_list['value']

    for item in dataset_list:
        item['workspace_id'] = workspace_id
    
    return dataset_list

def _add_column_to_json_def(json_def:dict, field_name:StructField, field_dtype:StructField) -> dict:
    """
    Add record with expected name and type and nullability
    """
    print(f'Checking if {field_dtype} is equal to StructType()')
    if field_dtype == StringType():
        print(f'field type is StructType')
        value = None

    json_def[field_name] = value

    return json_def


def structure_table_schema(json_def:dict, schema:StructType) -> dict:
    """
    This is to set the schema of the table to the expected schema.  User will pass in a list of columns

    args:
    json_def = {'id': '689c98b3-bf23-4f2b-ae34-d51277bf9261',
                'name': 'UNC-Workshop',
                'state': 'Active',
                'type': 'Workspace',
                'capacityId': 'AD343E36-F335-4BA3-B261-B739F7E950B0'}
    
    schema = StructType([
            StructField('capacityId', StringType(), True),
            StructField('id', StringType(), True),
            StructField('name', StringType(), True),
            StructField('state', StringType(), True),
            StructField('type', StringType(), True),
            StructField('domain', StringType(), True)
        ])

    """
    # create a list of column names
    json_def_column_list = json_def.keys()

    for field in schema.fields:
        # validate each column exists
        if field.name not in json_def_column_list:
            print(f'Field Name: {field.name} not in schema with expected type: {field.dataType}')
            json_def = _add_column_to_json_def(json_def, field.name, field.dataType)
            print(f'Field Name: {field.name} added to schema')
        
    return json_def




# def create_or_merge_datasets_tbl(lakehouse_tables_abfs_path: str, clean_datasets_list: list):
#     """
#     Create or merge into the datasets_tbl Delta table.
#     If the table exists, merge new data based on 'id'.
#     If it doesn't exist, create it.
#     """
#     df = spark.createDataFrame(clean_datasets_list)
#     table_path = f"{lakehouse_tables_abfs_path}/datasets_tbl"

#     if DeltaTable.isDeltaTable(spark, table_path):
#         delta_table = DeltaTable.forPath(spark, table_path)

#         # Merge based on 'id' (or another unique key)
#         delta_table.alias("target").merge(
#             df.alias("source"),
#             "target.id = source.id"
#      ).whenMatchedUpdateAll() \
#       .whenNotMatchedInsertAll() \
#         .execute()
#     else:
#         df.write.format("delta").mode("overwrite").save(table_path)


def create_merge_delta_tbl(lakehouse_tables_abfss_path:str, table_name:str, clean_data_list:list, schema:StructType, merge_str:str):
    """
    Create or merge into the workspace_tbl Delta Table

    lakehouse_tables_abfss_path:str: The abfss path of the lakehouse


    """
    df = spark.createDataFrame(clean_data_list, schema)
    table_path = f'{lakehouse_tables_abfss_path}/{table_name}'

    if DeltaTable.isDeltaTable(spark, table_path):
        delta_table = DeltaTable.forPath(spark, table_path)

        # merge based on id
        delta_table.alias('target').merge(
            df.alias('source'),
            merge_str
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        df.write.format('delta').mode('overwrite').save(table_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Testing

json_def = {'id': '689c98b3-bf23-4f2b-ae34-d51277bf9261',
 'name': 'UNC-Workshop',
 'state': 'Active',
 'type': 'Workspace',
 'capacityId': 'AD343E36-F335-4BA3-B261-B739F7E950B0'}

schema = StructType([
    StructField('capacityId', StringType(), True),
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('state', StringType(), True),
    StructField('type', StringType(), True),
    StructField('domain', StringType(), True)
])

cleaned_json_def = structure_table_schema(json_def,schema)

cleaned_json_def

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_json_def

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token = await get_api_token_via_akv_async(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# current error in cleaned_workspace_json = structure_table_schema(workspaces_json['workspaces'],workspace_tbl_schema)
# get token
token = await get_api_token_via_akv_async(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

capacity_id = 'AD343E36-F335-4BA3-B261-B739F7E950B0'

# define timeouts and host connection config
timeout = aiohttp.ClientTimeout(total=300, connect=60)
connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)

workspace_tbl_schema = StructType([
    StructField('capacityId', StringType(), True),
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('state', StringType(), True),
    StructField('type', StringType(), True),
    StructField('domain', StringType(), True)
])
workspace_tbl_name = 'workspace_tbl'
workspace_tbl_merge_logic = 'target.id = source.id'

datasets_tbl_schema = StructType([
    StructField('id', StringType(), True),
    StructField('type', StringType(), True),
    StructField('displayName', StringType(), True),
    StructField('description', StringType(), True),
    StructField('workspaceId', StringType(), True)
])
datasets_tbl_name = 'datasets_tbl'
datasets_tbl_merge_logic = 'target.id = source.id'



refresh_attempt_schema_strings = StructType([
    StructField("attemptId", IntegerType(), True),
    StructField("startTime", StringType(),  True),
    StructField("endTime",   StringType(),  True),
    StructField("type",      StringType(),  True),
])

dataset_refresh_schema = StructType([
    StructField('requestId', StringType(), True),
    StructField('id', StringType(), True),
    StructField('refreshType', StringType(), True),
    StructField('startTime', TimestampType(), True),
    StructField('endTime', TimestampType(), True),
    StructField('status', StringType(), True),
    StructField('refreshAttempts', ArrayType(refresh_attempt_schema_strings), True)
])

cleaned_workspaces_json_list = []
ws_datasets = []

async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
    # get all workspaces
    workspaces_json = await get_all_workspaces_async(session, capacity_id, token)

    ### Clean workspaces_json
    for workspace in workspaces_json['workspaces']:
        cleaned_workspace_json = structure_table_schema(workspace,workspace_tbl_schema)
        cleaned_workspaces_json_list.append(cleaned_workspace_json)
    
    ### create tables (alter later to merge)
    create_merge_delta_tbl(lakehouse_tables_abfs_path, workspace_tbl_name, cleaned_workspaces_json_list, workspace_tbl_schema, workspace_tbl_merge_logic)

    # for each workspace id in the workspaces_json['workspaces], get all the datasets/semantic models for it
    for ws in workspaces_json['workspaces']:
        # get all semantic models
        models = await get_all_datasets_in_workspace_async(session, ws['id'], token)
        if models:
            ws_datasets.extend(models)
        

    ### create datasets table (merge)
    create_merge_delta_tbl(lakehouse_tables_abfs_path, datasets_tbl_name, ws_datasets, datasets_tbl_schema, datasets_tbl_merge_logic)
    
    # # Get refresh info
    refresh_info = await get_dataset_refresh_info_async(session, workspace_id, dataset_id, dataset_refresh_schema, token)
    # print("Refresh info retrieved")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

refresh_info_example = refresh_info

refresh_info_example.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

refresh_info_example['refreshAttempts'].values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_user_to_workspace(group_id:str, workspace_list:list, api_token:str):
    """
    
    https://api.powerbi.com/v1.0/myorg/groups/{groupId}/users
    """
    for workspace_id in workspace_list:
        print(f'Attempting to add user group ID:{group_id} to workspace:{workspace_id}')

        url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/users'

        post_body = {
            "identifier": "{group_id}",
            "groupUserAccessRight": "Admin",
            "principalType": "Group"
        }

        headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
        }    

        response = requests.post(url, headers=headers, json=post_body)
        print(response.status_code)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
