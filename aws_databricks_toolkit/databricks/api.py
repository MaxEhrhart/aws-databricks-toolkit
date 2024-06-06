# encoding: latin1
import json
import requests

from ..custom_objects import SandboxResources
from ..aws.secrets_manager import get_secret
token = get_secret('databricks')['token']



def list_jobs(token):
    url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/jobs/list'
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    result = json.loads(response.text)['jobs']
    return result


def list_instance_pool(token: str = None):
    url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/instance-pools/list'
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    result = json.loads(response.text)['instance_pools']
    return result


def list_availability_zones(token: str = None):
    url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/clusters/list-zones'
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    result = json.loads(response.text)
    return result


def add_permissions_job(job_id, token: str = None):
    try:
        print(f'init {job_id=}')
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.0/permissions/jobs/{job_id}'
        response = requests.patch(request_url, headers={"Authorization": f"Bearer {token}"}, json={"access_control_list": [{'group_name': 'data_engineering', "permission_level": "CAN_MANAGE"}]})
        print(f"{job_id=} OK")
        return True
    except Exception as e:
        print(e)
        return False


def cancel_job(job_id, token: str = None):
    try:
        print(f'{job_id=}')
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/runs/cancel-all'
        response = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json={'job_id': job_id})
        assert response.status_code == 200
        print(f"{job_id=} OK")
        return True
    except Exception as e:
        print(e, response)  # , json.dumps(response.json(), indent=4))
        return False


def run_job(job_id, token: str = None):
    try:
        print(f'init {job_id=}')
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/run-now'
        response = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json={'job_id': job_id})
        assert response.status_code == 200
        print(f"{job_id=} OK")
        return True
    except Exception as e:
        print(e, json.dumps(response.json(), indent=4))
        return False


def delete_job(job_id, token: str = None):
    try:
        print(f'init {job_id=}')
        request_url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/jobs/delete'
        response = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json={"job_id": job_id})
        assert response.status_code == 200
        print(f"{job_id=} OK")
        return True
    except Exception as e:
        print(e)
        return False


def repair_run(run_id, latest_repair_id=None, token: str = None):
    try:
        print(f'init {run_id=}')
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/runs/repair'
        response = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json={'run_id': run_id, "latest_repair_id": latest_repair_id, "rerun_all_failed_tasks": True})
        assert response.status_code == 200
        print(f"{run_id=} OK")
        return True
    except Exception as e:
        print(e, json.dumps(response.json(), indent=4))
        return False


def get_job(job_id, token: str = None):
    try:
        print(f'init {job_id=}')
        url = 'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/get'
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, json={'job_id': job_id})
        assert response.status_code == 200
        result = json.loads(response.text)
        print(f"{job_id=} OK")
        return result
    except Exception as e:
        print(e, json.dumps(response.json(), indent=4))
        return False


def get_job_runs(job_id, params = None, token: str = None):
    url = 'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/runs/list'
    payload = {'job_id': job_id} if params is None else {'job_id': job_id, **params}
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    result = json.loads(response.text)
    return result


def update_job(job_id, payload, reset: bool = False, token: str = None):
    try:
        print(f'init {job_id=}')
        if not reset:
            request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/update'
        else:
            request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/reset'
        response = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json=payload)
        assert response.status_code == 200
        print(f"{job_id=} OK")
        return True
    except Exception as e:
        print(e)
        print(json.dumps(response.json(), indent=4))
        return False


def update_job_schedule(job_id: int, schedule: dict, token: str = None):
    """
    Atualiza agendamento do job.
    job_id: int = id do job a ser atualizado
    schedule: dict = dicionario, exemplo:
    {
        "quartz_cron_expression": "20 30 * * * ?",
        "timezone_id": "Europe/London",
        "pause_status": "PAUSED"
    }
    """
    payload = {"job_id": job_id, "new_settings": {"schedule": schedule}}
    result = update_job(job_id, payload, token)


def create_cluster_pool(payload, token: str = None):
    try:
        print(f'init {payload["instance_pool_name"]=}')
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.0/instance-pools/create'
        response = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json=payload)
        assert response.status_code == 200
        print(f'{payload["instance_pool_name"]=} OK')
        return True
    except Exception as e:
        print(e, json.dumps(response.json(), indent=4))
        return False


def get_existing_storage_credential(base_url, token):
    endpoint = "/api/2.1/unity-catalog/storage-credentials"

    # Cabeçalho com o token Bearer
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("Obtendo credenciais existentes...")
    response = requests.get(f"{base_url}{endpoint}", headers=headers)

    if response.status_code != 200:
        print("Falha ao obter credenciais existentes:", response.text)
        return set()

    credentials_data = response.json().get("storage_credentials", [])
    existing_credentials = set(credential["name"] for credential in credentials_data)

    return existing_credentials


def create_storage_credential(base_url, token, resource: SandboxResources, existing_credentials: list):
    endpoint = "/api/2.1/unity-catalog/storage-credentials"

    credential_name = f'sandbox-{resource.area_name.replace("_", "-")}'

    # Verificar se a credencial já existe
    if credential_name in existing_credentials:
        print(f"A credencial '{credential_name}' já existe. Pulando criação.")
        return

    # Cabeçalho com o token Bearer
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "name": credential_name,
        "comment": f"Sandbox {resource.area_name}",
        "read_only": False,
        "aws_iam_role": {
            "role_arn": resource.function_arn_unity_catalog
        },
        "skip_validation": False
    }

    print(f"Criando storage credential para o sandbox {credential_name}")
    response = requests.post(f"{base_url}{endpoint}", headers=headers, json=payload)

    if response.status_code == 200:
        print("Credencial criada com sucesso.")
    else:
        print("Falha ao criar a credencial:", response.text)


def list_external_locations(base_url, token):
    endpoint = "/api/2.1/unity-catalog/external-locations"

    # Cabeçalho com o token Bearer
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("Listando locais externos...")
    response = requests.get(f"{base_url}{endpoint}", headers=headers)

    if response.status_code != 200:
        print("Falha ao listar locais externos:", response.text)
        return []

    locations_data = response.json().get("external_locations", [])

    return locations_data


def create_external_location(base_url, token, resource: SandboxResources):
    endpoint = "/api/2.1/unity-catalog/external-locations"

    # Cabeçalho com o token Bearer
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    location_name = f"sandbox-{resource.area_name.replace('_', '-')}"

    # Verificar se o local externo já existe
    existing_locations = {location['name'] for location in list_external_locations(base_url, token)}
    if location_name in existing_locations:
        print(f"O local externo '{location_name}' já existe. Pulando criação.")
        return

    payload = {
        "name": location_name,
        "url": f"s3://{resource.bucket_name}",
        "credential_name": location_name,
        "read_only": False,
        "comment": f"Sandbox {resource.area_name}"
    }

    print(f"Criando local externo '{payload['name']}'...")
    response = requests.post(f"{base_url}{endpoint}", headers=headers, json=payload)

    if response.status_code == 200:
        print("Local externo criado com sucesso.")
    else:
        print("Falha ao criar o local externo:", response.text)


def list_sql_warehouses():
    # Headers for the API calls
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    DATABRICKS_INSTANCE = "https://digio-dw-data.cloud.databricks.com"
    url = f'{DATABRICKS_INSTANCE}/api/2.0/sql/warehouses'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        warehouses = response.json().get('warehouses', [])
        return [warehouse['name'] for warehouse in warehouses]
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []


def get_sql_warehouse_access_permissions(warehouse_id):
    # Headers for the API calls
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    DATABRICKS_INSTANCE = "https://digio-dw-data.cloud.databricks.com"
    url = f'{DATABRICKS_INSTANCE}/api/2.0/permissions/warehouses/{warehouse_id}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        permissions = response.json()
        group_names = [entry['group_name'] for entry in permissions.get('access_control_list', []) if
                       'group_name' in entry]
        return group_names
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []


def create_sql_warehouse(name, cluster_size, max_num_clusters, min_num_clusters, auto_stop_minutes, tags):
    # Headers for the API calls
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    DATABRICKS_INSTANCE = "https://digio-dw-data.cloud.databricks.com"
    url = f'{DATABRICKS_INSTANCE}/api/2.0/sql/warehouses'
    payload = {
        'name': name,
        'cluster_size': cluster_size,
        'max_num_clusters': max_num_clusters,
        'min_num_clusters': min_num_clusters,
        'auto_stop_mins': auto_stop_minutes,
        'tags': {
            'custom_tags': tags
        },
        'spot_instance_policy': 'POLICY_UNSPECIFIED',  # default value
        'enable_serverless_compute': True
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        warehouse = response.json()
        print(f"SQL Warehouse created successfully: {warehouse}")
        return warehouse['id']
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None


def set_sql_warehouses_access_permissions(warehouse_id, access_control_list):
    # Headers for the API calls
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    DATABRICKS_INSTANCE = "https://digio-dw-data.cloud.databricks.com"
    url = f'{DATABRICKS_INSTANCE}/api/2.0/permissions/warehouses/{warehouse_id}'
    payload = {
        'object_id': warehouse_id,
        'object_type': 'sqlWarehouse',
        'access_control_list': access_control_list
    }
    response = requests.patch(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        print("Access permissions set successfully")
    else:
        print(f"Error: {response.status_code}, {response.text}")


def stop_sql_warehouse(warehouse_id):
    # Headers for the API calls
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    DATABRICKS_INSTANCE = "https://digio-dw-data.cloud.databricks.com"
    url = f'{DATABRICKS_INSTANCE}/api/2.0/sql/warehouses/{warehouse_id}/stop'
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        print("SQL Warehouse stopped successfully")
    else:
        print(f"Error: {response.status_code}, {response.text}")


def list_cluster_policies_name_id(base_url, token):
    endpoint = "/api/2.0/policies/clusters/list"

    # Cabeçalho com o token Bearer
    headers = {
        "Authorization": f"Bearer {token}"
    }

    print("Listando políticas de cluster...")
    response = requests.get(f"{base_url}{endpoint}", headers=headers)

    if response.status_code == 200:
        policies = response.json().get("policies", [])
        if policies:
            print("Políticas de cluster encontradas:")
            for policy in policies:
                print(f"Nome: {policy['name']}, ID: {policy['policy_id']}")
        else:
            print("Nenhuma política de cluster encontrada.")
    else:
        print("Falha ao listar políticas de cluster:", response.text)


def update_cluster_policy_permissions(base_url, token, policy_name, resource: SandboxResources):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Etapa 1: Buscar o ID da política de cluster com base no nome
    list_policies_endpoint = "/api/2.0/policies/clusters/list"
    response = requests.get(f"{base_url}{list_policies_endpoint}", headers=headers)

    if response.status_code != 200:
        print("Falha ao listar políticas de cluster:", response.text)
        return

    policies = response.json().get('policies', [])
    cluster_policy_id = None
    for policy in policies:
        if policy['name'] == policy_name:
            cluster_policy_id = policy['policy_id']
            break

    if not cluster_policy_id:
        print(f"Política de cluster '{policy_name}' não encontrada.")
        return

    # Etapa 2: Definir as novas permissões
    new_permissions = [
        {
            "user_name": "data@digio.com.br",
            "permission_level": "CAN_USE"
        },
        {
            "group_name": resource.area_name,
            "permission_level": "CAN_USE"
        }
    ]

    payload = {
        "access_control_list": new_permissions
    }

    # Etapa 3: Atualizar as permissões da política de cluster
    update_permissions_endpoint = f"/api/2.0/permissions/cluster-policies/{cluster_policy_id}"
    print(f"Atualizando permissões da política de cluster '{policy_name}' (ID: {cluster_policy_id})...")
    response = requests.patch(f"{base_url}{update_permissions_endpoint}", headers=headers, json=payload)

    if response.status_code == 200:
        print("Permissões atualizadas com sucesso.")
    else:
        print("Falha ao atualizar permissões:", response.text)


if __name__ == '__main__':
    pass
