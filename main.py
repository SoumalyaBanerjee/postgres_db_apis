from fastapi import FastAPI, Request
import psycopg2
import datetime

app = FastAPI()

# PostgreSQL connection parameters
hostname = "uq2gn.h.filess.io"
database = "Thrift_moonneckit"
port = "5433"
username = "Thrift_moonneckit"
password = "6c06e56f7a2b5a62aa415133bb7e336ae6fcb491"

@app.post("/insert")
async def insert_sensor_data(request: Request):
    data = await request.json()

    # Validate required fields
    temperature = data.get("temperature")
    timestamp = data.get("timestamp")
    billet_no = data.get("billet_no")

    if temperature is None or timestamp is None or billet_no is None:
        return {"error": "Missing one or more required fields: temperature, timestamp, billet_no"}

    # Connect to the database
    conn = psycopg2.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )
    cur = conn.cursor()

    try:
        # Insert the data into thrift.thermal_sensor_input
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
