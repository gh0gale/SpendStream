import { startIngestion, finishIngestion } from "../api/ingestion";
import "./EmailIngestion.css";

export default function EmailIngestion({ setRun }) {
  const handleRun = async () => {
    const run = await startIngestion("email");
    setRun(run);
    await finishIngestion(run.run_id);
  };

  return (
    <div className="card">
      <h3>Email Ingestion</h3>
      <button onClick={handleRun}>Run Email Ingestion</button>
    </div>
  );
}
