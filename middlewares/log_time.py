import time

from fastapi import Request
from loguru import logger


async def log_request_time(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    # logger.info(f"Request path: {request.url.path=}: {process_time=:.2f}ms")
    logger.info(f"{request.url.path=}|{process_time=:.2f}ms")
    return response
