from fastapi import FastAPI, Request, HTTPException
from supabase import create_client
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

from fastapi import UploadFile, File
from datetime import datetime

import pandas as pd
import io
import re 
from datetime import datetime
import requests

import base64


from fastapi.responses import RedirectResponse
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials


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

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")

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


# file upload
def clean_amount(val):
    try:
        val = str(val)
        val = re.sub(r"[^\d.]", "", val)  # remove ₹, commas, etc
        return float(val)
    except:
        return None

def parse_date_safe(date_val):
    try:
        return pd.to_datetime(date_val).to_pydatetime()
    except:
        return None

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

    # 🧠 Identify columns
    date_col = next((c for c in df.columns if "date" in c), None)
    narration_col = next((c for c in df.columns if "narration" in c or "description" in c), None)
    withdrawal_col = next((c for c in df.columns if "withdrawal" in c or "debit" in c), None)

    if not date_col or not withdrawal_col:
        return {"error": "Required columns not found"}

    # ✅ Drop useless rows early
    df = df.dropna(subset=[withdrawal_col])

    transactions = []

    for _, row in df.iterrows():
        try:
            amount = clean_amount(row.get(withdrawal_col))

            if not amount or amount <= 0:
                continue

            timestamp = parse_date_safe(row.get(date_col))
            if not timestamp:
                continue

            narration = str(row.get(narration_col, "")).strip()

            transaction = {
                "user_id": user.id,
                "amount": amount,
                "receiver": narration[:100] if narration else "UNKNOWN",
                "transaction_type": "debit",
                "timestamp": timestamp.isoformat(),
                "source": "file",
                "raw_text": narration
            }

            transactions.append(transaction)

        except Exception as e:
            print("Row error:", e)

    # 🚫 OPTIONAL: dedup before insert (basic)
    unique_transactions = {(
        t["amount"],
        t["timestamp"],
        t["receiver"]
    ): t for t in transactions}.values()

    # 📦 Insert
    if unique_transactions:
        supabase.table("transactions").insert(list(unique_transactions)).execute()

    return {
        "message": "File processed successfully",
        "transactions_found": len(unique_transactions)
    }


# mails load
@app.get("/auth/google")
def auth_google():
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    redirect_uri = "http://127.0.0.1:8000/auth/callback"

    scope = "https://www.googleapis.com/auth/gmail.readonly"

    google_auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope={scope}"
        f"&access_type=offline"
        f"&prompt=consent"
    )

    return RedirectResponse(google_auth_url)


# TEMP storage (we fix later)
user_tokens = {}
gmail_tokens = {}

@app.get("/auth/google/callback")
def google_callback(request: Request):
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
        redirect_uri=GOOGLE_REDIRECT_URI,
    )

    flow.fetch_token(authorization_response=str(request.url))

    credentials = flow.credentials

    # ⚠️ TEMP (we improve later)
    global user_tokens
    user_tokens = {
        "access_token": credentials.token,
        "refresh_token": credentials.refresh_token
    }

    return {"message": "Gmail connected successfully"}


#after gmail choosed and redirect
from fastapi.responses import RedirectResponse

@app.get("/auth/callback")
def auth_callback(code: str):
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = "http://127.0.0.1:8000/auth/callback"

    token_url = "https://oauth2.googleapis.com/token"

    data = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }

    response = requests.post(token_url, data=data)
    token_data = response.json()

    gmail_tokens["access_token"] = token_data.get("access_token")
    gmail_tokens["refresh_token"] = token_data.get("refresh_token")

    # 🔥 REDIRECT BACK TO FRONTEND
    return RedirectResponse("http://localhost:5173")



@app.get("/fetch-gmail")
def fetch_gmail(request: Request):
    user = get_user_from_token(request)

    # 🔐 Supabase session
    token = request.headers.get("Authorization").split(" ")[1]
    supabase.auth.set_session(token, token)

    access_token = gmail_tokens.get("access_token")

    if not access_token:
        return {"error": "Gmail not connected"}

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    # 🧠 STEP 1: GET LAST FETCH TIME
    sync_res = supabase.table("gmail_sync").select("*").eq("user_id", user.id).execute()

    if sync_res.data:
        last_fetch = sync_res.data[0]["last_fetched"]

        # 🔥 Incremental fetch
        query = f"from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) (debited OR spent OR txn OR transaction) after:{last_fetch[:10]}"

        print("INCREMENTAL FETCH FROM:", last_fetch)

    else:
        # 🔥 First time → current month only
        now = datetime.utcnow()
        first_day = now.replace(day=1).strftime("%Y/%m/%d")

        query = f"from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) (debited OR spent OR txn OR transaction) after:{first_day}"

        print("FIRST TIME FETCH FROM:", first_day)

    # 🔁 PAGINATION
    all_messages = []
    next_page_token = None

    for _ in range(5):  # limit pages
        url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages?q={query}&maxResults=10"

        if next_page_token:
            url += f"&pageToken={next_page_token}"

        res = requests.get(url, headers=headers)
        data = res.json()

        if "messages" in data:
            all_messages.extend(data["messages"])

        next_page_token = data.get("nextPageToken")

        if not next_page_token:
            break

    print("TOTAL MESSAGES:", len(all_messages))

    transactions = []

    # 🔍 PROCESS EMAILS
    for msg in all_messages:
        try:
            msg_id = msg["id"]

            msg_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}"
            msg_res = requests.get(msg_url, headers=headers)
            msg_data = msg_res.json()

            payload = msg_data.get("payload", {})

            # 🔥 BODY EXTRACTION
            def extract_body(payload):
                if "parts" in payload:
                    for part in payload["parts"]:
                        mime = part.get("mimeType", "")
                        if mime in ["text/plain", "text/html"]:
                            data = part["body"].get("data")
                            if data:
                                return data
                        if "parts" in part:
                            result = extract_body(part)
                            if result:
                                return result
                else:
                    return payload.get("body", {}).get("data")
                return None

            raw_body = extract_body(payload)

            if not raw_body:
                continue

            body = base64.urlsafe_b64decode(raw_body).decode("utf-8", errors="ignore")

            # ✅ ONLY debit
            if not re.search(r"(?i)debited|spent|paid", body):
                continue

            # ✅ AMOUNT
            match = re.search(
                r"(?i)(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2}).*?(debited|spent|paid)",
                body
            )

            if not match:
                match = re.search(
                    r"(?i)(debited|spent|paid).*?(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2})",
                    body
                )

            if not match:
                continue

            amount = float(match.group(1 if match.lastindex == 2 else 2).replace(",", ""))

            # ✅ RECEIVER
            receiver_match = re.search(r"to\s+(.*?)\s+on", body, re.IGNORECASE)
            receiver = receiver_match.group(1) if receiver_match else "UNKNOWN"

            transaction = {
                "user_id": user.id,
                "amount": amount,
                "receiver": receiver,
                "transaction_type": "debit",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "gmail",
                "raw_text": body[:200]
            }

            transactions.append(transaction)

        except Exception as e:
            print("Email parse error:", e)

    # 📦 INSERT
    if transactions:
        supabase.table("transactions").insert(transactions).execute()

    # 🧠 STEP 2: UPDATE LAST FETCH TIME
    now_iso = datetime.utcnow().isoformat()

    if sync_res.data:
        supabase.table("gmail_sync").update({
            "last_fetched": now_iso
        }).eq("user_id", user.id).execute()
    else:
        supabase.table("gmail_sync").insert({
            "user_id": user.id,
            "last_fetched": now_iso
        }).execute()

    return {
        "transactions_found": len(transactions)
    }