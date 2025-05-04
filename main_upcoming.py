from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
import psycopg2
from typing import Optional

app = FastAPI()

# === DB Config ===
hostname = "uq2gn.h.filess.io"
database = "Thrift_moonneckit"
port = "5433"
username = "Thrift_moonneckit"
password = "6c06e56f7a2b5a62aa415133bb7e336ae6fcb491"

# === Auth Config ===
SECRET_KEY = "supersecretkey"  # replace with a stronger secret in production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# === Password Hashing ===
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# === Token Handler ===
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# === Dummy Users DB ===
fake_users_db = {
    "user1": {
        "username": "user1",
        "full_name": "User One",
        "hashed_password": pwd_context.hash("secret"),
        "role": "user"
    },
    "admin": {
        "username": "admin",
        "full_name": "Admin User",
        "hashed_password": pwd_context.hash("admin123"),
        "role": "admin"
    }
}

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def authenticate_user(username: str, password: str):
    user = fake_users_db.get(username)
    if not user or not verify_password(password, user["hashed_password"]):
        return None
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(status_code=401, detail="Invalid token")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            raise credentials_exception
        user = fake_users_db.get(username)
        if not user:
            raise credentials_exception
        return user
    except JWTError:
        raise credentials_exception

def require_admin(user: dict = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

# === Routes ===

@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/insert")
async def insert_sensor_data(request: Request, user: dict = Depends(get_current_user)):
    data = await request.json()
    temperature = data.get("temperature")
    timestamp = data.get("timestamp")
    billet_no = data.get("billet_no")

    if temperature is None or timestamp is None or billet_no is None:
        raise HTTPException(status_code=400, detail="Missing required fields")

    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password, port=port)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO thrift.thermal_sensor_input (temperature, "timestamp", billet_no)
            VALUES (%s, %s, %s)
        """, (temperature, timestamp, billet_no))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

    return {"status": "success", "data": data}

@app.get("/select/by_billet")
async def select_by_billet(billet_no: int, user: dict = Depends(get_current_user)):
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password, port=port)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM thrift.thermal_sensor_input WHERE billet_no = %s
        """, (billet_no,))
        result = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return {"results": result}

@app.get("/select/by_timestamp")
async def select_by_timestamp(start: str, end: str, user: dict = Depends(get_current_user)):
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password, port=port)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM thrift.thermal_sensor_input 
            WHERE "timestamp" BETWEEN %s AND %s
        """, (start, end))
        result = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return {"results": result}

@app.get("/select/by_temperature")
async def select_by_temperature(threshold: float, comparator: str = "gt", user: dict = Depends(get_current_user)):
    if comparator not in ["gt", "lt", "eq"]:
        raise HTTPException(status_code=400, detail="Invalid comparator")
    op = { "gt": ">", "lt": "<", "eq": "=" }[comparator]

    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password, port=port)
    cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT * FROM thrift.thermal_sensor_input 
            WHERE temperature {op} %s
        """, (threshold,))
        result = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return {"results": result}

@app.post("/admin/run_query")
async def admin_query(request: Request, user: dict = Depends(require_admin)):
    data = await request.json()
    query = data.get("query")
    if not query or not query.lower().strip().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=password, port=port)
    cur = conn.cursor()
    try:
        cur.execute(query)
        result = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return {"results": result}
