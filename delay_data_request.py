from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import requests

class ReportPollingSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, endpoint_url, trigger_report_task_id, *args, **kwargs):
        super(ReportPollingSensor, self).__init__(*args, **kwargs)
        self.endpoint_url_template = endpoint_url
        self.trigger_report_task_id = trigger_report_task_id

    def poke(self, context):
        report_id = context['ti'].xcom_pull(task_ids=self.trigger_report_task_id)
        resp = requests.get(self.endpoint_url_template.format(report_id=report_id))
        resp.raise_for_status()
        resp_json = resp.json()
        if resp_json["status"] == "ok":
            return True
        return False

def trigger_report_generating():
    resp = requests.post(url=TRIGGER_GENERATING_URL)
    resp.raise_for_status()
    resp_json = resp.json()
    if resp_json["status"] != "ok":
        raise Exception(resp_json["error_msg"])
    return resp_json["report_id"]

def download_report(trigger_report_task_id, report_destination, **kwargs):
    # get the report download url
    report_id = kwargs['ti'].xcom_pull(task_ids=trigger_report_task_id)
    resp = requests.get(REPORT_POLLING_URL.format(report_id=report_id))
    resp.raise_for_status()
    resp_json = resp.json()
    if resp_json["status"] != "ok":
        raise Exception(resp_json)

    # download and write file
    resp = requests.get(resp_json["report_download_url"])
    resp.raise_for_status()

    with open(f"{report_destination}/report", 'wb') as file:
        file.write(resp.content)


from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

TRIGGER_GENERATING_URL = "external.api/generate_report/{client_id}"
REPORT_POLLING_URL = "external.api/download_report/{report_id}"
POKE_INTERVAL = timedelta(minutes=90)
REPORT_DESTINATION = "/data/reports"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hourly_report_dowload',
    default_args=default_args,
    description='Hourly report download DAG',
    schedule_interval=timedelta(hours=1),
):
    trigger_report_generating_task_id = "trigger_report_generating"
    trigger_report_generating_task = PythonOperator(
        task_id=trigger_report_generating_task_id,
        python_callable=trigger_report_generating,
    )

    polling_sensor = ReportPollingSensor(
        endpoint_url_template=REPORT_POLLING_URL,
        trigger_report_task_id=trigger_report_generating_task_id,
        poke_interval=POKE_INTERVAL,
        timeout=3 * POKE_INTERVAL,
        mode="reschedule" # because the poke interval is long (90 mins) and it will better to release the worker resource during waiting.
    )

    download_report_task = PythonOperator(
        task_id='download_report',
        python_callable=download_report,
        op_kwargs={
            "trigger_report_task_id": trigger_report_generating_task_id
        }
    )

    trigger_report_generating_task_id >> polling_sensor >> download_report_task