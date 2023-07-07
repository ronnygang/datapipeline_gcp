from __future__ import annotations
from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import base64
import requests

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

def make_composer2_web_server_request(url: str, method: str = "GET", 
                                      **kwargs: Any) -> google.auth.transport.Response:
    authed_session = AuthorizedSession(CREDENTIALS)
    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)

def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}
    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )
    if response.status_code == 403:
        raise requests.HTTPError(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            f"{response.headers} / {response.text}"
        )
    elif response.status_code != 200:
        response.raise_for_status()
    else:
        return response.text


def consume_message(event, context):
    if 'data' in event:
        message = event['data']
        if isinstance(message, bytes):
            message = message.decode('utf-8')
        print('Mensaje recibido:', message)
        
        decoded_message = base64.b64decode(message).decode('utf-8')        
        file_name = decoded_message
                
        print("message: ", file_name)
        
        file_extension = file_name.split('.')[-1]
        object_name = file_name.split('_')[0]  # Dividir el string en partes utilizando el guion bajo como separador
        table_name = object_name.split('/')[-1]
        INSTANCE = 'https://6f5e4512a8324ac19b5b05192553bccf-dot-us-central1.composer.googleusercontent.com'
        DATA = {
                'conf': {}
            }
        DAG_ID = f'load_{table_name}_{object_name}'
        if file_extension.upper() == 'TXT':
            if table_name.upper() == 'TRANSACTIONS':                
                return trigger_dag(INSTANCE, DAG_ID, DATA)
            if table_name.upper() == 'CAMPAIGNS':
                return trigger_dag(INSTANCE, DAG_ID, DATA)
        elif file_extension.upper() == 'CSV':
            if table_name.upper() == 'TRANSACTIONS':                
                return trigger_dag(INSTANCE, DAG_ID, DATA)
            if table_name.upper() == 'CAMPAIGNS':
                return trigger_dag(INSTANCE, DAG_ID, DATA)
        else:
            return trigger_dag(INSTANCE, 'airflow_monitoring', DATA)
        
        return 'OK'
    else:
        return 'No data in the event'
