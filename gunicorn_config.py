import os

from loguru import logger

logger.add('logs/app.log', rotation='1 day', retention='7 days')

workers = 2
bind = "0.0.0.0:3399"
worker_class = "uvicorn.workers.UvicornWorker"

def post_fork(server, worker):
    worker_id = worker.pid % workers

    logger.info(f"Worker {worker_id} pid: {worker.pid}")

    os.environ["WORKER_ID"] = str(worker_id)
    logger.info(f"Worker {worker_id} started")
    if worker_id == 0:
        os.environ["INIT_CRONS"] = "true"
    else:
        os.environ["INIT_CRONS"] = "false"

