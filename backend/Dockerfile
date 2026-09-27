# SpendStream backend (FastAPI + ETL + ML categoriser).
# Runs on Linux so Windows Smart App Control never sees the compiled
# numpy / scipy / regex extensions it blocks on the host.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# requirements.txt is the hand-kept list; requirements.lock pins every package
# (regenerate: docker run --rm spendstream-backend pip freeze > backend/requirements.lock).
COPY requirements.txt requirements.lock ./
RUN pip install -r requirements.lock

COPY . .

RUN useradd --create-home --uid 1000 app \
    && chown -R app:app /app
USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/ping', timeout=4)"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
