const BASE_URL = "http://localhost:8000";

export async function startIngestion(source) {
  const res = await fetch(
    `${BASE_URL}/ingestion/start?source=${source}`,
    { method: "POST" }
  );
  return res.json();
}

export async function finishIngestion(runId) {
  const res = await fetch(
    `${BASE_URL}/ingestion/finish?run_id=${runId}`,
    { method: "POST" }
  );
  return res.json();
}

export async function uploadFile(file) {
  const formData = new FormData();
  formData.append("file", file);

  const res = await fetch(`${BASE_URL}/files/upload`, {
    method: "POST",
    body: formData,
  });

  return res.json();
}

export async function triggerEmailIngestion() {
  const res = await fetch(
    `${BASE_URL}/email/trigger-ingestion`,
    { method: "POST" }
  );
  return res.json();
}

export async function getIngestionStatus() {
  const res = await fetch(
    `${BASE_URL}/email/ingestion-status`,
    { method: "GET" }
  );
  return res.json();
}
