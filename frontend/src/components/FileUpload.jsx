import { useState } from "react";
import { uploadFile } from "../api/ingestion";
import "./FileUpload.css";

export default function FileUpload() {
  const [file, setFile] = useState(null);

  const handleUpload = async () => {
    if (!file) return;
    await uploadFile(file);
    alert("File uploaded successfully");
  };

  return (
    <div className="card">
      <h3>Upload Statement</h3>

      <input
        type="file"
        accept=".csv,.pdf"
        onChange={(e) => setFile(e.target.files[0])}
      />

      <button onClick={handleUpload}>Upload</button>
    </div>
  );
}
