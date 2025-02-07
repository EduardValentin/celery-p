import sys

from celery.result import AsyncResult
from celery_app import app

task_id = sys.argv[1]

print(f"Revoking task ID: {task_id}")

result = AsyncResult(task_id, app=app)

result.revoke(terminate=True)

print(f"Task Info: {result}")


