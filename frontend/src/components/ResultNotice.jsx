import styles from './ResultNotice.module.css'

// The result of a correction. It stays until the next action or until closed:
// no timer decides when the user has read it.
export default function ResultNotice({ result, onClose, onRetry }) {
  if (!result) return null
  return (
    <div className={`${styles.notice} ${result.error ? styles.error : ''}`} role="status">
      <p>{result.text}</p>
      <div className={styles.actions}>
        {result.error && onRetry && <button type="button" className="btn" onClick={onRetry}>Try again</button>}
        <button type="button" className="btn btn-text" onClick={onClose}>Close</button>
      </div>
    </div>
  )
}
