from fastapi import APIRouter, Depends

from databases.apgdb import get_db, PgDB

router = APIRouter()


@router.get("/hello")
async def hello(db: PgDB = Depends(get_db)):
    sql = "SELECT now() as curr_time;"
    result, columns = await db.query(sql, return_records=False)
    return {
        "message": "hello from api v1!!",
        "current_time": result[0][0]
    }


