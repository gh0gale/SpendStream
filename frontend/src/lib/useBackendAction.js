import { useCallback, useEffect, useRef, useState } from 'react'

// The free-tier backend sleeps. A call with no reply after SLOW_AFTER_MS is
// shown as "the server is starting", with the real seconds waited so far.
const SLOW_AFTER_MS = 3000

export function useBackendAction(action) {
  const [startedAt, setStartedAt] = useState(null)
  const [now, setNow] = useState(() => Date.now())
  const actionRef = useRef(action)
  useEffect(() => { actionRef.current = action })

  useEffect(() => {
    if (!startedAt) return
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [startedAt])

  const run = useCallback(async (...args) => {
    const start = Date.now()
    setStartedAt(start)
    setNow(start)
    try {
      return await actionRef.current(...args)
    } finally {
      setStartedAt(null)
    }
  }, [])

  const waited = startedAt ? now - startedAt : 0
  return {
    run,
    pending: Boolean(startedAt),
    waking: waited >= SLOW_AFTER_MS,
    seconds: Math.floor(waited / 1000),
  }
}
