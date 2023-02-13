# encoding: latin1
import json
import requests
# from ..aws.secrets_manager import get_secret
# token = get_secret('databricks')['token']


def list_jobs(token):
    import requests
    import json

    url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/jobs/list'
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    result = json.loads(response.text)['jobs']
    return result


def list_instance_pool(token: str = None):
    import requests
    import json

    url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/instance-pools/list'
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    result = json.loads(response.text)['instance_pools']
    return result


def add_permissions(job_id, token: str = None):
    try:
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.0/permissions/jobs/{job_id}'
        auth = requests.patch(request_url, headers={"Authorization": f"Bearer {token}"}, json={
            "access_control_list": [{'group_name': 'data_engineering', "permission_level": "CAN_MANAGE"}]})
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
        return True
    except Exception as e:
        print(e, json.dumps(response.json(), indent=4))
        return False


def delete_job(job_id, token: str = None):
    try:
        request_url = 'https://digio-dw-data.cloud.databricks.com/api/2.0/jobs/delete'
        auth = requests.post(request_url, headers={"Authorization": f"Bearer {token}"}, json={"job_id": job_id})
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
        return True
    except Exception as e:
        print(e, json.dumps(response.json(), indent=4))
        return False


def get_job(job_id, token: str = None):
    import requests
    import json
    url = 'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/get'
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, json={'job_id': job_id})
    result = json.loads(response.text)
    return result


def get_job_runs(job_id, params = None, token: str = None):
    import requests
    import json
    url = 'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/runs/list'
    payload = {'job_id': job_id} if params is None else {'job_id': job_id, **params}
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    result = json.loads(response.text)
    return result


def update_job(job_id, payload, token: str = None):
    try:
        print(f'init {job_id=}')
        request_url = f'https://digio-dw-data.cloud.databricks.com/api/2.1/jobs/update'
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


if __name__ == '__main__':
    pass
