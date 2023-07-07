from __future__ import annotations

from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession
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
        
def trigger_dag_gcf(event, context):
    file_name = event['name']
    parts = file_name.split('_')[0]
    table_name = parts.split('/')[-1]
    object_name = file_name.split('.')[-1]
    INSTANCE = 'https://783acb7571f14328adb7ebc713355964-dot-us-central1.composer.googleusercontent.com'
    DAG_ID = f'load_{table_name}_{object_name}'
    print(f'Executin dag_id: {DAG_ID}')
    #DAG_ID = 'composer_sample_trigger_response_dag'
    DATA = {
        'conf': {}
    }
    
    return trigger_dag(INSTANCE, DAG_ID, DATA)
