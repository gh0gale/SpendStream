# SpendStream: Project Documentation

This document provides a comprehensive overview of the SpendStream project, detailing the architecture, technologies, and specific engineering challenges resolved during development. This is intended to serve as a context source for generating a high-quality GitHub README.

## 1. Project Overview
SpendStream is an intelligent financial tracking system that automates expense categorization using Machine Learning. It syncs with a user's Gmail to detect bank alerts, processes them through a data pipeline, and provides a sleek dashboard for financial growth and insights.

## 2. Core Architecture: The Medallion Pipeline
The backend follows a Medallion (Multi-hop) architecture to ensure data integrity and traceability:

- **Raw Layer**: Unprocessed transaction data fetched via Gmail API (bank alerts) or manual CSV uploads.
- **Bronze Layer**: Data is cleaned and assigned a unique "fingerprint" (hash) based on amount, receiver, and date to prevent duplicates across sources.
- **Silver Layer**: Merchant names are normalized (e.g., stripping UPI handles), and the ML model assigns a category and confidence score.
- **Gold Layer**: Categorized transactions are aggregated into monthly summaries for the "Spend Tracker" visualizations.

## 3. Machine Learning & Categorization
The intelligence layer uses a hybrid approach for maximum accuracy:

- **Cold Start**: Uses **Sentence Transformers (`all-MiniLM-L6-v2`)** and cosine similarity against category "anchors" to categorize transactions with zero prior training data.
- **Supervised Learning**: A **Logistic Regression/SGDClassifier** trained on TF-IDF features (char and word n-grams) and transaction metadata (amount, time of day, day of week, frequency).
- **Online Learning Loop**: User corrections are captured in a `category_feedback` table. A background Celery task performs a "warm-start" refit of the model to learn user habits (e.g., learning that a specific personal UPI name is "Food").
- **Pattern Boosts**: Heuristic layer that boosts probability scores based on historical frequency and transaction amounts (e.g., detecting recurring subscriptions).

## 4. Technical Stack
- **Frontend**: React (Vite), Vanilla CSS, Supabase Auth.
- **Backend API**: FastAPI (Python).
- **Database**: Supabase (PostgreSQL).
- **Async Tasks**: Celery with Redis (Upstash) as the broker.
- **ML Libraries**: Scikit-learn (pinned to 1.7.2), Pandas, NumPy, Sentence-Transformers, Joblib.
- **Deployment**: Render (Backend), Vercel (Frontend).

## 5. Key Engineering Challenges & Solutions

### A. Process Isolation & Persistent Memory
**Challenge**: The ML model's "memory" (user history/corrections) was originally stored in-memory. In production, the Web Server and Celery Worker run in separate processes, meaning the worker couldn't "see" what the web app learned.
**Solution**: Migrated the `HistoryStore` and `UserOverrideStore` to be DB-backed. The ML prediction loop now queries Supabase for the latest user feedback before running inference.

### B. API Performance & 504 Timeouts
**Challenge**: Inline model retraining on manual corrections was blocking the FastAPI event loop, causing gateway timeouts.
**Solution**: Offloaded model retraining to a dedicated Celery task, allowing the API to respond instantly while the model updates in the background.

### C. Serialization & Compatibility
**Challenge**: NumPy types (`np.str_`, `np.float32`) returned by the ML model caused JSON serialization failures during database upserts.
**Solution**: Implemented a recursive casting layer in the ETL pipeline to convert all ML outputs to native Python types.

### D. Environment Synchronization
**Challenge**: Version mismatches between local and production `scikit-learn` caused silent prediction failures (everything categorized as "Other").
**Solution**: Pinned exact versions in `requirements.txt` and implemented an `os.path.getmtime` watcher to trigger hot-reloads of `.pkl` model files across server instances.

## 6. User Experience Overhaul
The project moved away from "Technical Jargon" (ML, Medallion, ETL) to a benefit-first marketing approach:
- **Connect**: Securely link Gmail.
- **Detect**: Instant transaction extraction.
- **Sort**: Automatic smart categorization.
- **Grow**: Data-driven financial insights.

## 7. Setup & Development
1. **Backend**: Install dependencies via `requirements.txt`, setup `.env` (Supabase, Google OAuth, Redis), and run with `uvicorn main:app`.
2. **Workers**: Run Celery worker: `celery -A tasks.celery worker --loglevel=info`.
3. **Frontend**: `npm install` and `npm run dev`.
