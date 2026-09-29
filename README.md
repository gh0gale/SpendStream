<div align="center">

# SpendStream

**Your bank alerts, sorted into a monthly spending picture. Correct a merchant once and it stays corrected.**

[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-20232A?style=flat-square&logo=react&logoColor=61DAFB)](https://reactjs.org/)
[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=flat-square&logo=supabase&logoColor=white)](https://supabase.com/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=flat-square&logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)

</div>

---

## What is SpendStream?

SpendStream reads the debit alerts your bank sends to Gmail, turns each one into a transaction, and sorts it into one of 13 categories. You see where the month's money went. When it gets a merchant wrong, you correct it once: every other payment to that payee moves with it, and every future one follows your choice.

| Step | What happens |
|------|-------------|
| **Connect** | Link Gmail through Google OAuth (read-only scope). Only bank-alert emails are searched |
| **Read** | Debits are parsed from the alerts. Credits, refunds, reversals, OTPs and declined payments are skipped |
| **Sort** | Your own rules first, then a classifier. Below 50% confidence a payment is left as Unsure |
| **Review** | A monthly dashboard, a transaction list filtered by month and category and a Needs review queue grouped by merchant |

Gmail is the only source. The first sync reads the current month; later syncs pick up from the last one, three times a day or on demand (once per 10 minutes).

---

## Architecture

```
Gmail API (bank alerts)
        │   tasks.py: list, download 4 at a time, retry, parse (gmail_parser.py)
        ▼
┌──────────────────┐
│ transactions     │  raw rows, unique per Gmail message id
└────────┬─────────┘
         ▼  etl.py work queues, 200 rows per batch
┌──────────────────┐
│ bronze           │  deduplicated by fingerprint of the message id
└────────┬─────────┘
         ▼
┌──────────────────┐
│ silver           │  cleaned merchant name, stable merchant_key, person or business,
│                  │  date in India time, then a category:
│                  │    1. your rule for this merchant_key
│                  │    2. the shared merchant directory (3+ users agreeing at 80%+)
│                  │    3. the model
└────────┬─────────┘
         ▼
┌──────────────────┐
│ gold (a view)    │  monthly totals per category, always current
└──────────────────┘
```

The browser reads its own rows straight from Supabase (row-level security on every table) and writes only through one Postgres function, `correct_category()`. The FastAPI backend handles Gmail OAuth, syncs and account deletion with the service-role key. Background work runs in FastAPI `BackgroundTasks`; there is no Celery or Redis.

---

## Categorisation

### Your corrections are rules

Each transaction carries a `merchant_key`: the UPI payee id when there is one (`upi:swiggy@icici`), else the cleaned name. A correction is saved as your rule for that key and applied, in the same database transaction, to your other payments to that payee. The pipeline checks your rules before anything else, so the model is never asked about that merchant again. Matching is exact: correcting "Sharma General Store" cannot touch "Sharma Medical".

Your corrections are never shared as such. Other users benefit only through the merchant directory, which holds a merchant only when at least 3 users agree at 80% or more, and never payments to people.

### The model

A logistic regression over TF-IDF character (2 to 5) and word (1 to 2) n-grams of the cleaned merchant name, plus amount and India-time hour and weekday. Payments to shop QR codes (Paytm, BharatPe, PhonePe merchant QRs and similar) carry a marker, so a shop under its owner's name is not read as a transfer to a person. The categoriser is stateless: no database access, no per-user memory. The feature code lives in one file (`backend/ml/features.py`) shared by training, evaluation and prediction.

**Measured on 312 real, hand-labelled debits the compare model never trained on: 87.8% accuracy (95% interval 84.3 to 91.3%), macro-F1 0.745.** The shipped model is trained on the same recipe plus those rows, so it cannot be scored on them.

### Retraining

- Corrections never retrain the model.
- `backend/retrain.sh` runs a gated retrain: a compare run scored on the golden set, then the ship run.
- A weekly GitHub Actions job (`weekly_retrain.py`) retrains only when the merchant directory holds at least 20 merchants and 100 transactions the live model has not seen, and ships only if the new model matches or beats the current baseline.

The trained model files are **not in this repository**: their vocabulary holds real payee names. They live in a private Supabase Storage bucket, and the API downloads the live model at startup (`backend/model_store.py`).

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | React 19, Vite 7, react-router 7, plain CSS (one design-system stylesheet plus CSS modules), Supabase JS |
| **Backend API** | FastAPI (Python 3.12), `requests` for Gmail and Google OAuth, Fernet-encrypted refresh tokens |
| **Database** | Supabase (Postgres) with row-level security; schema in `supabase/migrations/` |
| **ML** | scikit-learn 1.7.2 (pinned), NumPy, SciPy, joblib. No torch or sentence embeddings |
| **Hosting** | Backend on Render (Docker, `render.yaml`), frontend on Cloudflare Pages |
| **Automation** | GitHub Actions: CI, scheduled sync (3 times a day), weekly retrain, weekly encrypted database backup |

---

## Engineering Decisions

### Rules before the model
Corrections used to feed an in-memory history and an online refit of one shared model: one user's clicks shifted everyone's predictions, and the lookup keyed on the display name while prediction used the raw receiver, so corrections rarely matched. Now a correction is a per-user row in Postgres keyed on `merchant_key`, the model is stateless, and shared knowledge needs agreement from several users.

### A real accuracy number
Earlier accuracy figures were measured on the training templates. A golden set of real, hand-labelled debits, split by merchant so no merchant appears in both training and test, is the only score a model change is judged on. The model before September 2026 scored 38.6% on it.

### Fitting a small host
Sentence embeddings (torch plus MiniLM) made no difference to golden-set accuracy and cost about 400 MB. Dropping them took peak memory from about 606 MB to about 220 MB and the image from 627 MB to about 190 MB.

### Syncs that do not lose mail
Each sync is a `sync_jobs` row the dashboard polls. The cursor always moves forward; messages that fail to download (rate limits, server errors) are kept and retried on the next sync, so one bad message cannot make every sync re-read the whole window.

### Pinned scikit-learn
A version mismatch between training and serving once turned every prediction into "Other". scikit-learn is pinned to 1.7.2, the image installs exact versions from `requirements.lock`, and the loader refuses a model whose feature layout does not match the code.

---

## Getting Started

### Prerequisites
- Docker (or Python 3.12)
- Node.js 20+
- A Supabase project with the migrations in `supabase/migrations/` applied
- A Google Cloud project with the Gmail API and an OAuth web client; redirect URI `http://localhost:8000/auth/callback`
- A trained model in the Supabase bucket `models` (`python model_store.py push-model`), or `python train_model.py` for a templates-only model

### Backend

```bash
git clone https://github.com/gh0gale/SpendStream.git
cd SpendStream

cp backend/.env.example backend/.env
# Fill in SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_ROLE_KEY,
# GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, CRON_SECRET, TOKEN_ENCRYPTION_KEY
# (the file shows how to generate the last two). The API refuses to start without them.

docker compose up --build                              # API on :8000, reloads on edits
docker compose run --rm backend python cron_runner.py  # one-shot sync of every connected user
```

Without Docker, from `backend/`: `python -m venv .venv`, install `requirements.txt`, then `uvicorn main:app --reload`.

### Frontend

```bash
cd frontend
cp .env.example .env   # VITE_SUPABASE_URL, VITE_SUPABASE_ANON_KEY, VITE_API_URL
npm ci
npm run dev            # http://localhost:5173
```

### Tests

```bash
cd backend && sh run_tests.sh      # eight offline test scripts; no .env, network or database
sh supabase/tests/run_local.sh     # migrations and row-level security on a throwaway Postgres (Docker)
cd frontend && npm run lint && npm run build
```

---

## Project Structure

```
SpendStream/
├── backend/
│   ├── main.py               # FastAPI routes: OAuth, sync, cron, health, account deletion
│   ├── tasks.py              # Gmail sync jobs
│   ├── gmail_parser.py       # Bank alert -> transaction (pure functions)
│   ├── etl.py                # raw -> bronze -> silver -> categorised
│   ├── merchant_identity.py  # Clean name, merchant_key, person or business, shop QR
│   ├── config.py             # The only module that reads environment variables
│   ├── token_crypto.py       # Refresh-token encryption
│   ├── model_store.py        # Model files in the private Supabase bucket
│   ├── train_model.py        # Training
│   ├── evaluate_model.py     # Golden-set scoring (the release gate)
│   ├── build_golden_set.py   # Builds the golden set and the train/ship CSVs
│   ├── export_demo.py        # Anonymised demo data for the public pages
│   ├── weekly_retrain.py     # Automated gated retrain
│   ├── retrain.sh            # Local gated retrain
│   ├── cron_runner.py        # One-shot sync of all users
│   ├── test_*.py             # Standalone test scripts
│   └── ml/
│       ├── features.py       # The one feature definition
│       └── categoriser.py    # Stateless prediction
├── supabase/
│   ├── migrations/           # Schema, row-level security, correct_category(), apply_predictions()
│   └── tests/                # run_local.sh and SQL checks
├── frontend/src/
│   ├── pages/                # Home, How it works, Your data, Privacy, Terms, Login, Connect,
│   │                         # Dashboard, Transactions, Needs review, Account
│   ├── components/
│   └── lib/                  # Supabase client, API helper, data hooks
├── .github/workflows/        # ci.yml, sync.yml, retrain.yml, backup.yml
├── render.yaml               # Render Blueprint for the backend
└── compose.yaml
```

---
<div align="center">

Built with Python, React, and a lot of bank alerts.

</div>
