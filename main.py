import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from loguru import logger
from redis.asyncio import Redis
from setproctitle import setproctitle

from middlewares.error import ErrorHandlerMiddleware
from middlewares.log_time import log_request_time
from routes.v1.api import router as v1_api
from routes.v1.cap_reports import router as cap_reports_api
from routes.v1.sa_kpi import router as sa_kpi_api
from utils.crons import heartbeat, scheduler
from utils.utils import lock_file, unlock_file

load_dotenv()


@asynccontextmanager
async def lifespan(app_: FastAPI):
    # startup code here
    fd = lock_file('tmp/master.lock')
    lock_acquired = fd is not None
    logger.info(f"Lock acquired: {lock_acquired}")
    app_.state.is_master = lock_acquired
    app_.state.redis = Redis.from_url('redis://localhost')

    # lock the file tmp/master.lock
    # to prevent multiple instances of the same cron job
    # from running at the same time
    # from running at the same time
    async def start_crons():
        asyncio.create_task(heartbeat())

    if lock_acquired:
        logger.info('Lock acquired. Starting crons')
        scheduler.start()
        asyncio.create_task(start_crons())

    # redis = aioredis.from_url("redis://localhost")
    # FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")

    yield

    if lock_acquired:
        scheduler.shutdown()
        unlock_file(fd)
        logger.info("Lock released")

    await app_.state.redis.close()


app = FastAPI(lifespan=lifespan)

proctitle = os.getenv("proctitle", "__hawk_fast_api__")
setproctitle(proctitle)

app.add_middleware(ErrorHandlerMiddleware)

app.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"],
                   )

app.middleware("http")(log_request_time)

# routers
app.include_router(v1_api, prefix="/v1/api")

app.include_router(cap_reports_api, prefix="/v1/cap_reports")

app.include_router(sa_kpi_api, prefix="/v1/sa_kpi")


# some basic routes for testing
@app.get("/")
async def root():
    return {"message": "Hello from root."}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Check if this worker should initialize the cron jobs
if os.getenv("INIT_CRONS", "false").lower() == "true":
    logger.info(f"Initializing cron jobs")


@app.get("/set_state/{key}/{value}")
async def set_state(request: Request, key: str, value: str):
    await request.app.state.redis.set(key, value)
    return {"message": f"State set for {key} with value {value}"}


@app.get("/get_state/{key}")
async def get_state(request: Request, key: str):
    value = await request.app.state.redis.get(key)
    return {key: value}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8009)
