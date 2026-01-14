import "./RunStatus.css";

export default function RunStatus({ run }) {
  if (!run) return null;

  return (
    <div className="status">
      <h4>Latest Ingestion Run</h4>
      <p><strong>ID:</strong> {run.run_id}</p>
      <p><strong>Source:</strong> {run.source}</p>
      <p><strong>Status:</strong> {run.status}</p>
    </div>
  );
}
