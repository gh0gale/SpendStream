import { useState } from "react";
import { uploadFile, runFileIngestion } from "../api/ingestion";
import "./FileUpload.css";

export default function FileUpload() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleUpload = async () => {
    if (!file) return;
    await uploadFile(file);
    alert("File uploaded successfully");
  };

  const handleIngest = async () => {
    try {
      setLoading(true);
      await runFileIngestion();
      alert("File ingestion completed");
    } catch {
      alert("File ingestion failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card">
      <h3>Upload Statement</h3>

      <input
        type="file"
        accept=".csv,.pdf"
        onChange={(e) => setFile(e.target.files[0])}
      />

      <button className="secondary" onClick={handleUpload}>Upload File</button>

      {/* 👇 THIS IS THE BUTTON YOU ASKED FOR */}
      <button
        
        onClick={handleIngest}
        disabled={loading}
      >
        {loading ? "Ingesting..." : "Run File Ingestion"}
      </button>
    </div>
  );
}
