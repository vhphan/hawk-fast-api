import time

from fastapi import Request
from loguru import logger


async def log_request_time(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    logger.info(f"{request.url.path=}|{process_time=:.2f}seconds")
    return response
