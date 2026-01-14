import { useState } from "react";
import EmailIngestion from "../components/EmailIngestion";
import FileUpload from "../components/FileUpload";
import RunStatus from "../components/RunStatus";
import "./Dashboard.css";

export default function Dashboard() {
  const [run, setRun] = useState(null);

  return (
    <div className="dashboard">
      <h1>SpendStream</h1>
      <p className="subtitle">Data Ingestion Dashboard</p>

      <EmailIngestion setRun={setRun} />
      <FileUpload />
      <RunStatus run={run} />
    </div>
  );
}
