from fastapi import APIRouter
from starlette.responses import FileResponse

from routes.v1.settings import BUSY_HOUR_FOLDER

router = APIRouter()


# create a route that downloads a file from the server
@router.get('/hello/{name}')
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@router.get("/download-csv/{file_name}")
async def download_file(file_name: str):
    try:
        file_path = f"{BUSY_HOUR_FOLDER}/{file_name}"
        return FileResponse(path=file_path, filename=file_name)
    except Exception as e:
        return {"error": f"Error while downloading file: {e}"}

