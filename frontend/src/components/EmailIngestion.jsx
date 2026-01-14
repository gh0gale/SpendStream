import { useState, useEffect } from "react";
import { triggerEmailIngestion, getIngestionStatus } from "../api/ingestion";
import "./EmailIngestion.css";

export default function EmailIngestion({ setRun }) {
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [runId, setRunId] = useState(null);

  const pollStatus = async () => {
    try {
      const result = await getIngestionStatus();
      
      if (result.status === "running") {
        setMessage("Email ingestion in progress...");
        setTimeout(pollStatus, 2000); // Poll every 2 seconds
      } else if (result.status === "success") {
        setMessage("✅ " + result.message);
        setError("");
        setLoading(false);
        setRun({ run_id: result.run_id || runId, source: "email", status: result.status });
      } else if (result.status === "error") {
        setMessage("❌ " + result.message);
        setError(result.error || "Unknown error");
        setLoading(false);
        setRun({ run_id: result.run_id || runId, source: "email", status: result.status });
      } else {
        setLoading(false);
      }
    } catch (err) {
      console.error("Error polling status:", err);
    }
  };

  const handleRun = async () => {
    setLoading(true);
    setMessage("Starting email ingestion...");
    setError("");
    
    try {
      const result = await triggerEmailIngestion();
      setMessage(result.message);
      setRunId(result.run_id);
      
      // Start polling for status
      setTimeout(pollStatus, 1000);
    } catch (error) {
      setError(error.message);
      setMessage("Error connecting to backend");
      setLoading(false);
    }
  };

  return (
    <div className="card">
      <h3>📧 Email Ingestion</h3>
      <p>Fetch emails from Gmail and store as JSON files</p>
      <button onClick={handleRun} disabled={loading}>
        {loading ? "Processing..." : "Run Email Ingestion"}
      </button>
      {message && <p className="message">{message}</p>}
      {error && <p className="error">{error}</p>}
    </div>
  );
}
