import traceback

from fastapi import Request
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
            return response
        except Exception as exc:
            logger.error(f"Unhandled error: {exc}")
            # print error details

            logger.error(f"Unhandled error: {exc}\n{traceback.format_exc()}")

            return JSONResponse(
                status_code=500,
                content=dict(message="An internal server error occurred.", error_details=str(exc))
            )
