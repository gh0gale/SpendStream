import { useEffect, useRef, useState } from 'react'
import { WakingNote } from './States'
import { formatDateTime, plural } from '../lib/format'
import { formatMonth } from '../lib/format'
import { minutesUntilSyncAllowed, nextHistoryMonth } from '../lib/useGmailSync'
import styles from './SyncLine.module.css'

// Is the data current, and can the user sync now? Every state here comes from
// gmail_sync or the sync_jobs row, never from a timer. The 10-minute limit
// between manual syncs is not shown up front: Sync now stays pressable, and a
// press inside the limit opens a small note saying when to try again.
export default function SyncLine({ sync }) {
  const { gmail, sync: job, start, startAction, startBackfill, backfillAction, checkAgain } = sync
  const [tooSoon, setTooSoon] = useState(null)   // minutes left for the note; 0 = not known
  const noteRef = useRef(null)

  const running = job?.state === 'running'
  const busy = gmail.status !== 'connected' || running || startAction.pending || backfillAction.pending
  const nextMonth = nextHistoryMonth(gmail.historyFrom)   // null at the limit
  const rateLimited = job?.state === 'failed' && job.status === 429

  useEffect(() => {
    if (tooSoon === null) return
    const close = (e) => { if (e.type === 'keydown' ? e.key === 'Escape' : !noteRef.current?.contains(e.target)) setTooSoon(null) }
    document.addEventListener('mousedown', close)
    document.addEventListener('keydown', close)
    return () => { document.removeEventListener('mousedown', close); document.removeEventListener('keydown', close) }
  }, [tooSoon])

  const handleSync = async () => {
    if (busy) return
    const wait = minutesUntilSyncAllowed(gmail.lastRequested, Date.now())
    if (wait > 0) { setTooSoon(wait); return }
    setTooSoon(null)
    // The backend can still refuse as too soon (another tab or device synced).
    if (await start() === 429) setTooSoon(0)
  }

  // Same rules as Sync now: one manual request per 10 minutes, shared.
  const handleBackfill = async () => {
    if (busy) return
    const wait = minutesUntilSyncAllowed(gmail.lastRequested, Date.now())
    if (wait > 0) { setTooSoon(wait); return }
    setTooSoon(null)
    if (await startBackfill() === 429) setTooSoon(0)
  }

  return (
    <div className={styles.line}>
      <div className={styles.row}>
        <p className="small" aria-live="polite">
          {gmail.status === 'loading' ? 'Checking the last sync'
            : gmail.lastSynced ? <>Last synced <span className="num">{formatDateTime(gmail.lastSynced)}</span></>
            : 'Not synced yet'}
          {running && <> · Syncing</>}
          {job?.state === 'succeeded' && <> · Found {plural(job.found, 'new payment')}</>}
          {job?.state === 'succeeded' && job.retrying > 0 && <> · {plural(job.retrying, 'email')} could not be read and will be retried</>}
        </p>
        <div className={styles.action} ref={noteRef}>
          <button type="button" className="btn" aria-disabled={busy} onClick={handleSync}
            aria-describedby={tooSoon !== null ? 'sync-too-soon' : undefined}>
            {startAction.pending && <span className="spinner" aria-hidden="true" />}
            {startAction.waking ? 'Starting server' : 'Sync now'}
          </button>
          {gmail.status === 'connected' && nextMonth !== null && (
            <button type="button" className="btn btn-text" aria-disabled={busy} onClick={handleBackfill}>
              {backfillAction.pending && <span className="spinner" aria-hidden="true" />}
              {backfillAction.waking ? 'Starting server' : nextMonth ? `Read ${formatMonth(nextMonth)}` : 'Read an earlier month'}
            </button>
          )}
          {tooSoon !== null && (
            <div id="sync-too-soon" className={styles.note} role="status">
              <p>You synced a few minutes ago. Try again in {tooSoon > 0 ? plural(tooSoon, 'minute') : 'a few minutes'}.</p>
              <button type="button" className="btn btn-text" onClick={() => setTooSoon(null)}>OK</button>
            </div>
          )}
        </div>
      </div>
      {running && <div className="progress-rule" role="presentation" />}
      <WakingNote action={startAction} />
      {job?.state === 'failed' && !rateLimited && (
        <p className={styles.failed} role="alert">
          {job.error}{' '}
          {job.error?.startsWith('Interrupted') && (
            <button type="button" className="btn btn-text" onClick={handleSync}>Sync again</button>
          )}
        </p>
      )}
      {job?.state === 'timeout' && (
        <p className="small" role="status">
          Still reading. This page will update when you come back.{' '}
          <button type="button" className="btn btn-text" onClick={checkAgain}>Check again</button>
        </p>
      )}
    </div>
  )
}
