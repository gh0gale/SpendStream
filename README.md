<div align="center">

<br />

```
███████╗██████╗ ███████╗███╗   ██╗██████╗ ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗
██╔════╝██╔══██╗██╔════╝████╗  ██║██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║
███████╗██████╔╝█████╗  ██╔██╗ ██║██║  ██║███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║
╚════██║██╔═══╝ ██╔══╝  ██║╚██╗██║██║  ██║╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║
███████║██║     ███████╗██║ ╚████║██████╔╝███████║   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║
╚══════╝╚═╝     ╚══════╝╚═╝  ╚═══╝╚═════╝ ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝
```

**Intelligent financial tracking that learns your spending habits — automatically.**

[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-20232A?style=flat-square&logo=react&logoColor=61DAFB)](https://reactjs.org/)
[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=flat-square&logo=supabase&logoColor=white)](https://supabase.com/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=flat-square&logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)


</div>

---

## What is SpendStream?

SpendStream syncs with your Gmail to automatically detect bank alerts, extract transactions, and categorize them using a self-learning ML model. The more you use it and correct it, the smarter it gets — adapting to *your* spending patterns, not a generic template.

### The four-step flow

| Step | What happens |
|------|-------------|
| 🔗 **Connect** | Securely link your Gmail via OAuth |
| 🔍 **Detect** | Transactions are extracted instantly from bank alert emails |
| 🗂️ **Sort** | An ML model auto-categorizes every transaction |
| 📈 **Grow** | Monthly summaries and dashboards surface spending insights |

---

## Architecture: The Medallion Pipeline

SpendStream processes data through a four-stage pipeline that guarantees integrity and full traceability from raw email to dashboard insight.

```
Gmail API / CSV Upload
        │
        ▼
┌─────────────────┐
│   RAW LAYER     │  Unprocessed transaction data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BRONZE LAYER   │  Deduplication via content hash (amount + receiver + date)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  Merchant normalization + ML categorization + confidence scoring
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   GOLD LAYER    │  Monthly aggregates → Spend Tracker visualizations
└─────────────────┘
```

---

## Machine Learning Engine

SpendStream uses a **hybrid ML approach** that works out of the box and improves over time.

### Cold Start (zero training data)
Uses **Sentence Transformers (`all-MiniLM-L6-v2`)** with cosine similarity against category "anchor" embeddings. No prior data needed — accurate from day one.

### Supervised Model
A **Logistic Regression / SGDClassifier** trained on:
- TF-IDF features (character and word n-grams on merchant names)
- Transaction metadata: amount, time of day, day of week, frequency

### Online Learning Loop
User corrections are stored in a `category_feedback` table. A background **Celery task** performs a warm-start refit, so the model progressively learns personal habits — e.g., recognizing a personal UPI handle as "Food" rather than "Transfer".

### Pattern Boosts
A heuristic layer boosts probability scores based on historical frequency and amount patterns, enabling automatic detection of recurring subscriptions.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | React (Vite), Vanilla CSS, Supabase Auth |
| **Backend API** | FastAPI (Python) |
| **Database** | Supabase (PostgreSQL) |
| **ML** | scikit-learn 1.7.2, Sentence-Transformers, Pandas, NumPy, Joblib |


---

## Engineering Decisions

### Process Isolation & Persistent ML Memory
The ML model's "memory" (user history and corrections) was originally in-process RAM. In production, the API server and Celery worker are separate processes — the worker couldn't see what the API learned.

**Fix:** Migrated `HistoryStore` and `UserOverrideStore` to be fully DB-backed. The inference loop now queries Supabase for the latest user feedback before every prediction.

### API Performance & 504 Timeouts
Inline model retraining on user corrections was blocking the FastAPI async event loop, causing gateway timeouts under load.

**Fix:** Offloaded all retraining to a dedicated Celery task. The API responds instantly; model updates happen in the background.

### Serialization Failures
NumPy types (`np.str_`, `np.float32`) returned by the ML model caused JSON serialization errors during database upserts.

**Fix:** A recursive type-casting layer in the ETL pipeline converts all ML outputs to native Python types before they leave the inference layer.

### Environment Synchronization
`scikit-learn` version mismatches between local and production environments caused silent prediction failures where every transaction was categorized as "Other".

**Fix:** Pinned exact versions in `requirements.txt`. Added an `os.path.getmtime` watcher to hot-reload `.pkl` model files across server instances when models are updated.

---

## Getting Started

### Prerequisites
- Python 3.10+
- Node.js 18+
- A Supabase project
- Google Cloud project with Gmail API + OAuth enabled


### Backend

```bash
# Clone the repo
git clone https://github.com/your-username/spendstream.git
cd spendstream/backend

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Fill in: SUPABASE_URL, SUPABASE_KEY, GOOGLE_CLIENT_ID,
#          GOOGLE_CLIENT_SECRET, REDIS_URL

# Start the API server
uvicorn main:app --reload

# In a separate terminal, start the Celery worker
celery -A tasks.celery worker --loglevel=info
```

### Frontend

```bash
cd ../frontend

# Install dependencies
npm install

# Start the dev server
npm run dev
```

---

## Project Structure

```
spendstream/
├── backend/
│   ├── main.py              # FastAPI app & routes
│   ├── tasks.py             # Celery task definitions
│   ├── pipeline/
│   │   ├── raw.py           # Gmail API ingestion & CSV parsing
│   │   ├── bronze.py        # Deduplication & fingerprinting
│   │   ├── silver.py        # ML categorization
│   │   └── gold.py          # Aggregation layer
│   ├── ml/
│   │   ├── embeddings.py    # Sentence Transformer cold-start
│   │   ├── classifier.py    # SGDClassifier + TF-IDF
│   │   └── boosts.py        # Heuristic pattern layer
│   ├── stores/
│   │   ├── history.py       # DB-backed HistoryStore
│   │   └── overrides.py     # DB-backed UserOverrideStore
│   └── requirements.txt
└── frontend/
    ├── src/
    │   ├── components/      # React components
    │   └── pages/           # Dashboard, Connect, Insights
    └── package.json
```

---
<div align="center">

Built with Python, React, and a lot of bank alerts.

</div>
