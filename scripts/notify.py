import os
import requests

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def notify_success(context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    message = f"✅ SUCCESS: DAG {dag_id} completed (run_id={run_id})"

    print("Sending success alert to Slack...")
    print(message)

    if SLACK_WEBHOOK_URL:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )
        print("Slack status:", response.status_code)


def notify_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    message = f"❌ FAILURE: Task {task_id} in DAG {dag_id} failed"

    print("Sending failure alert to Slack...")
    print(message)

    if SLACK_WEBHOOK_URL:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )
        print("Slack status:", response.status_code)