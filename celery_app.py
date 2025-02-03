from time import sleep
from celery import Celery
from kombu.utils.functional import random

app = Celery(
    "tasks",
    broker="amqp://user:password@localhost:5672//",
    backend="db+mysql+pymysql://celery_user:celery_password@localhost/celery_db"
)

app.conf.broker_connection_retry_on_startup = True

@app.task
def add(x, y):
    sleep(random.randint(10,30))
    return x + y
