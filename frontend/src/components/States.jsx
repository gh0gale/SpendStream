// Shared async states. A failed read is an error, never an empty state.

export function ErrorNotice({ title, detail, onRetry, retryLabel = 'Try again' }) {
  return (
    <div className="notice notice-error" role="alert">
      <p className="notice-title">{title}</p>
      {detail && <p className="muted small">{detail}</p>}
      {onRetry && <button type="button" className="btn" onClick={onRetry}>{retryLabel}</button>}
    </div>
  )
}

// Shown under a backend action that has had no reply for a few seconds.
export function WakingNote({ action }) {
  if (!action.waking) return null
  return (
    <p className="small muted" role="status">
      The server is starting, this can take up to a minute. Waiting {action.seconds} s
    </p>
  )
}

export function SkeletonBlock({ height = 16, width = '100%' }) {
  return <div className="skeleton" style={{ height, width }} />
}

export function SkeletonRows({ rows = 6, height = 20 }) {
  return (
    <div aria-busy="true" style={{ display: 'grid', gap: 14 }}>
      <span className="visually-hidden">Loading</span>
      {Array.from({ length: rows }, (_, i) => <SkeletonBlock key={i} height={height} />)}
    </div>
  )
}
