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

# Configure logging for better error tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

async def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
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

async def get_dataset_refresh_info(workspace_id:str, dataset_id:str, api_token:str)->pd.DataFrame:
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

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            try:
                return pd.DataFrame(await response.json()['value'])
            except Exception as e:
                logger.error(f"Error in get_dataset_refresh_info: {e}")
                return None
        
async def start_dataset_refresh(workspace_id:str, dataset_id:str, api_token:str):
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

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as response:
            if response.status_code >=200 and response.status_code <300:
                print(f'Dataset Refresh request to workspace id:{workspace_id} and dataset id:{dataset_id} sent successfully')
            else:
                logger.error(f"Error in start_dataset_refresh: {response.status}")
            return response

async def cancel_dataset_refresh(workspace_id:str, dataset_id:str, refresh_id:str, api_token:str):
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

    async with aiohttp.ClientSession() as session:
        async with session.delete(url, headers=headers) as response:
            if response.status_code==409:
                print(f'Dataset Refresh already in a completed state; cannot cancel')
            elif response.status_code >=200 and response.status_code <300:
                print(f'Dataset Refresh cancelled successfully')
            else:
                logger.error(f"Error in cancel_dataset_refresh: {response.status}")
            return response

async def get_all_connections(api_token:str):
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

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status_code >=200 and response.status_code < 300:
                return await response.json()
            else:
                logger.error(f"Error in get_all_connections: {response.status}")
                return None

async def get_all_workspaces(api_token:str)-> json:
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

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status_code >=200 and response.status_code < 300:
                return await response.json()
            else:
                logger.error(f"Error in get_all_workspaces: {response.status}")
                return None

async def get_all_datasets_in_workspace(workspace_id:str, api_token:str):
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

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status_code >=200 and response.status_code < 300:
                return await response.json()
            else:
                logger.error(f"Error in get_all_datasets_in_workspace: {response.status}")
                return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get oauth token
token = await get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Dataset/SM Refresh Info
dataset_refresh_history = await get_dataset_refresh_info(workspace_id, dataset_id, token)

dataset_refresh_history

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Start Dataset/SM Refresh
# https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group

resp = await start_dataset_refresh(workspace_id, dataset_id, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cancel Dataset/SM Refresh
# https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/cancel-refresh-in-group
refresh_id = '2b2abe5c-330e-436b-bcd9-8c099254bc4a'

cancel_resp = await cancel_dataset_refresh(workspace_id, dataset_id, refresh_id, token)


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

workspace_json = await get_all_workspaces(token)

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

dataset_response = await get_all_datasets_in_workspace('21695bc6-4aeb-41ae-bbbd-93d8858e7665', token)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

async def get_dataset_refresh_info_async(session: aiohttp.ClientSession, workspace_id: str, dataset_id: str, api_token: str) -> pd.DataFrame:
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
            return pd.DataFrame(data.get('value', []))
    except aiohttp.ClientError as e:
        logger.error(f"Error getting dataset refresh info: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

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

async def get_all_workspaces_async(session: aiohttp.ClientSession, api_token: str) -> Optional[Dict[str, Any]]:
    """
    Async version of get_all_workspaces
    
    session: aiohttp.ClientSession for making HTTP requests
    api_token:str: The API Token used to authenticate with the APIs
    
    returns:
        Dict containing workspace data or None if error
    """
    url = 'https://api.fabric.microsoft.com/v1/admin/workspaces'
    
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
    """
    url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels'
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logger.error(f"Error getting datasets in workspace {workspace_id}: {str(e)}")
        raise

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

# EXAMPLE USAGE FUNCTIONS ********************

async def main_async_example():
    """
    Example of how to use the async functions
    """
    # Get token (this part is still sync due to notebookutils)
    token = await get_api_token_via_akv_async(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)
    
    # Example 1: Single operations
    timeout = aiohttp.ClientTimeout(total=300, connect=60)
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Get refresh info
        refresh_info = await get_dataset_refresh_info_async(session, workspace_id, dataset_id, token)
        print("Refresh info retrieved")
        
        # Start a refresh
        refresh_result = await start_dataset_refresh_async(session, workspace_id, dataset_id, token)
        print(f"Refresh start result: {refresh_result}")
    
    # Example 2: Bulk operations
    workspace_dataset_pairs = [
        (workspace_id, dataset_id),
        ('another-workspace-id', 'another-dataset-id')
    ]
    
    bulk_refresh_info = await get_multiple_datasets_refresh_info_async(workspace_dataset_pairs, token)
    print(f"Got refresh info for {len(bulk_refresh_info)} datasets")
    
    # Example 3: Multiple workspace operations
    bulk_results = await bulk_workspace_operations_async(token)
    print(f"Bulk operations completed: {list(bulk_results.keys())}")

# Keep original sync functions for backward compatibility
