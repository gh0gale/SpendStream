import { useState } from "react";
import { uploadFile, runFileIngestion } from "../api/ingestion";
import "./FileUpload.css";

export default function FileUpload({ setRun }) {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const handleUpload = async () => {
    if (!file) return;

    try {
      setLoading(true);
      await uploadFile(file);
      setMessage("✅ File uploaded successfully");
      setError("");
    } catch {
      setError("❌ File upload failed");
    } finally {
      setLoading(false);
    }
  };

  const handleIngest = async () => {
    try {
      setLoading(true);
      setMessage("📂 File ingestion in progress...");
      setError("");

      const result = await runFileIngestion();

      setMessage("✅ " + result.message);
      if (setRun) {
        setRun({ source: "file", status: result.status });
      }

    } catch (err) {
      setError("❌ " + (err.message || "File ingestion failed"));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card">
      <h3>📂 File Ingestion</h3>

      <input
        type="file"
        accept=".csv,.pdf"
        disabled={loading}
        onChange={(e) => setFile(e.target.files[0])}
      />

      <button className="secondary" onClick={handleUpload} disabled={loading}>
        Upload File
      </button>

      <button onClick={handleIngest} disabled={loading}>
        {loading ? "Processing..." : "Run File Ingestion"}
      </button>

      {message && <p className="message">{message}</p>}
      {error && <p className="error">{error}</p>}
    </div>
  );
}
