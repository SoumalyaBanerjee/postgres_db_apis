from fastapi import FastAPI, Request, Query
import psycopg2
from datetime import datetime
from typing import Optional
from fastapi import HTTPException
from pydantic import BaseModel, validator
from psycopg2.extras import RealDictCursor



app = FastAPI()

# PostgreSQL connection parameters
hostname = "uq2gn.h.filess.io"
database = "Thrift_moonneckit"
port = "5433"
username = "Thrift_moonneckit"
password = "6c06e56f7a2b5a62aa415133bb7e336ae6fcb491"

def get_connection():
    return psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )

@app.post("/insert")
async def insert_sensor_data(request: Request):
    data = await request.json()
    temperature = data.get("temperature")
    timestamp = data.get("timestamp")
    billet_no = data.get("billet_no")

    if temperature is None or timestamp is None or billet_no is None:
        return {"error": "Missing one or more required fields: temperature, timestamp, billet_no"}

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO thrift.thermal_sensor_input (temperature, "timestamp", billet_no)
            VALUES (%s, %s, %s)
        """, (temperature, timestamp, billet_no))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

    return {"status": "success", "data": data}

# SELECT by billet_no
@app.get("/select/by_billet")
def get_by_billet(billet_no: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM thrift.thermal_sensor_input WHERE billet_no = %s
        """, (billet_no,))
        result = cur.fetchall()
    except Exception as e:
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

    return {"results": result}

# Request body model
class TimestampRequest(BaseModel):
    start: datetime
    end: datetime

    @validator("end")
    def validate_range(cls, v, values):
        if "start" in values and v < values["start"]:
            raise ValueError("End must be after start")
        return v

@app.post("/select/by_timestamp")
def get_by_timestamp(data: TimestampRequest):
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)  # <-- returns dicts instead of tuples

        cur.execute("""
            SELECT temperature, timestamp, billet_no 
            FROM thrift.thermal_sensor_input
            WHERE "timestamp" >= %s AND "timestamp" <= %s
        """, (data.start, data.end))

        result = cur.fetchall()  # list of dicts
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

    return {"results": result}

# SELECT by temperature threshold
@app.get("/select/by_temperature")
def get_by_temperature(threshold: float, comparator: str = Query("gt", enum=["gt", "lt", "eq"])):
    conn = get_connection()
    cur = conn.cursor()
    try:
        if comparator == "gt":
            cur.execute("""SELECT * FROM thrift.thermal_sensor_input WHERE temperature > %s""", (threshold,))
        elif comparator == "lt":
            cur.execute("""SELECT * FROM thrift.thermal_sensor_input WHERE temperature < %s""", (threshold,))
        else:
            cur.execute("""SELECT * FROM thrift.thermal_sensor_input WHERE temperature = %s""", (threshold,))
        result = cur.fetchall()
    except Exception as e:
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

    return {"results": result}

# OPTIONAL: Run custom SQL query (⚠️ Use with caution)
@app.post("/admin/run_query")
async def run_custom_query(request: Request):
    data = await request.json()
    query = data.get("query")

    # Very basic safeguard - customize this for security
    if not query or not query.lower().startswith("select"):
        return {"error": "Only SELECT queries are allowed"}

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        result = cur.fetchall()
    except Exception as e:
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

    return {"results": result}


@app.put("/update/temperature")
async def update_temperature(request: Request):
    data = await request.json()
    billet_no = data.get("billet_no")
    new_temperature = data.get("temperature")

    if billet_no is None or new_temperature is None:
        raise HTTPException(status_code=400, detail="Missing billet_no or temperature")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE thrift.thermal_sensor_input
            SET temperature = %s
            WHERE billet_no = %s
        """, (new_temperature, billet_no))
        conn.commit()

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="No record found for given billet_no")

    except Exception as e:
        conn.rollback()
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

    return {"status": "updated", "billet_no": billet_no, "new_temperature": new_temperature}

@app.delete("/delete/by_billet")
def delete_by_billet(billet_no: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM thrift.thermal_sensor_input
            WHERE billet_no = %s
        """, (billet_no,))
        conn.commit()

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="No record found for given billet_no")

    except Exception as e:
        conn.rollback()
        return {"error": str(e)}
    finally:
        cur.close()
        conn.close()

    return {"status": "deleted", "billet_no": billet_no}


