const BASE_URL = "http://localhost:8000";

// Email
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

// Files
export const uploadFile = async (file) => {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(
    `${BASE_URL}/file/upload`, {
    method: "POST",
    body: formData
  });

  if (!response.ok) {
    throw new Error("Upload failed");
  }

  return response.json();
};

export async function runFileIngestion() {
  const res = await fetch(
    `${BASE_URL}/file/trigger-ingestion`, {
    method: "POST"
  });

  if (!res.ok) {
    throw new Error("File ingestion failed");
  }

  return res.json();
}
