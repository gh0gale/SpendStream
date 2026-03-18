from fastapi import FastAPI, Request, HTTPException
from supabase import create_client
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

from fastapi import UploadFile, File
from datetime import datetime

import pandas as pd
import io
from datetime import datetime


load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_ANON_KEY")
)

# 🔐 Helper function
def get_user_from_token(request: Request):
    auth_header = request.headers.get("Authorization")

    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing token")

    token = auth_header.split(" ")[1]

    try:
        user = supabase.auth.get_user(token)
        return user.user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


@app.get("/protected")
def protected_route(request: Request):
    user = get_user_from_token(request)

    return {
        "message": "You are authenticated",
        "user_id": user.id,
        "email": user.email
    }



@app.post("/upload-file")
async def upload_file(request: Request, file: UploadFile = File(...)):
    user = get_user_from_token(request)

    token = request.headers.get("Authorization").split(" ")[1]
    supabase.auth.set_session(token, token)

    contents = await file.read()

    # 📂 Detect file type
    if file.filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(contents))
    else:
        df = pd.read_excel(io.BytesIO(contents))

    # 🔥 Normalize column names
    df.columns = [col.strip().lower() for col in df.columns]

    print("COLUMNS:", df.columns)

    # 🧠 Identify columns dynamically
    date_col = next((c for c in df.columns if "date" in c), None)
    narration_col = next((c for c in df.columns if "narration" in c or "description" in c), None)
    withdrawal_col = next((c for c in df.columns if "withdrawal" in c or "debit" in c), None)
    deposit_col = next((c for c in df.columns if "deposit" in c or "credit" in c), None)

    if not date_col or not withdrawal_col:
        return {"error": "Required columns not found"}

    transactions = []

    for _, row in df.iterrows():
        try:
            withdrawal = row.get(withdrawal_col)

            # ✅ ONLY withdrawal (debit)
            if pd.isna(withdrawal):
                continue

            amount = float(str(withdrawal).replace(",", "").strip())

            if amount <= 0:
                continue

            date_val = row.get(date_col)
            narration = row.get(narration_col, "")

            # 🧾 Parse date safely
            if isinstance(date_val, str):
                timestamp = datetime.strptime(date_val, "%d/%m/%y")
            else:
                timestamp = pd.to_datetime(date_val)

            transaction = {
                "user_id": user.id,
                "amount": amount,
                "receiver": str(narration)[:100] if narration else "UNKNOWN",
                "transaction_type": "debit",
                "timestamp": timestamp.isoformat(),
                "source": "file",
                "raw_text": str(narration)
            }

            transactions.append(transaction)

        except Exception as e:
            print("Row error:", e)

    # 📦 Insert into Supabase
    if transactions:
        supabase.table("transactions").insert(transactions).execute()

    return {
        "message": "File processed successfully",
        "transactions_found": len(transactions)
    }