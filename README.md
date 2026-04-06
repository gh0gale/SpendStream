# 🚀 SpendStream

<div align="center">
  
  *An autonomous financial intelligence system that turns raw transaction data into actionable growth insights.*

  [![React](https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB)](https://reactjs.org/)
  [![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
  [![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
  [![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)](https://supabase.com/)
  [![scikit-learn](https://img.shields.io/badge/scikit--learn-%23F7931E.svg?style=for-the-badge&logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)
  [![Celery](https://img.shields.io/badge/celery-%2337814A.svg?style=for-the-badge&logo=celery&logoColor=white)](https://docs.celeryq.dev/)

</div>

---

## ✨ The Experience

SpendStream moves away from manual data entry and technical jargon, focusing entirely on a seamless, benefit-first financial flow:

* 🔗 **Connect:** Securely link your Gmail to listen for bank alerts.
* 🔍 **Detect:** Instant, automatic transaction extraction.
* 🧠 **Sort:** Smart categorization powered by adaptive Machine Learning.
* 📈 **Grow:** Beautiful, data-driven dashboards for financial insights.

*(Screenshot Placeholder: Add a clean image of your dashboard here)*
---

## 🏗️ Core Architecture: Medallion Pipeline

To ensure absolute data integrity, traceability, and duplicate prevention, the backend is structured around a Medallion (Multi-hop) data pipeline.

```mermaid
graph LR
    A[Gmail API / CSV] -->|Raw Layer| B(Bronze Layer)
    B -->|Cleaned & Fingerprinted| C{Silver Layer}
    C -->|ML Categorized| D[(Gold Layer)]
    D --> E[React Dashboard]
    
    classDef layer fill:#f9f9f9,stroke:#333,stroke-width:2px;
    class B,C,D layer;
