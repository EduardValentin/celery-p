from celery.result import AsyncResult
from celery_app import add, app

task = add.delay(4,4)


task_id = task.task_id 

result = AsyncResult(task_id, app=app)

print(f"Task ID: {task_id}")
print(f"State: {result.state}")
print(f"Result: {result.result}")  # Output of the task
print(f"Traceback: {result.traceback}")  # If failed, this shows the error traceback
