import { useCallback, useEffect, useState } from 'react'

// Runs an async read and keeps its result. `fetcher` must be memoised
// (useCallback); a new fetcher, or reload(), starts a new read. While a read
// is open, status is 'loading' and the previous data stays available, so a
// refreshed list does not flash empty. A failed read is 'error', never empty.
export function useLoad(fetcher) {
  const [version, setVersion] = useState(0)
  const [result, setResult] = useState({ fetcher: null, version: -1 })

  useEffect(() => {
    let cancelled = false
    fetcher().then(
      (data) => { if (!cancelled) setResult({ fetcher, version, status: 'ready', data }) },
      (err) => { if (!cancelled) setResult(r => ({ ...r, fetcher, version, status: 'error', detail: err.message })) },
    )
    return () => { cancelled = true }
  }, [fetcher, version])

  const reload = useCallback(() => setVersion(v => v + 1), [])
  const current = result.fetcher === fetcher && result.version === version

  return {
    status: current ? result.status : 'loading',
    data: result.data,
    detail: current ? result.detail : undefined,
    reload,
  }
}

// Supabase returns { data, error }; useLoad expects a throw on error.
export async function must(query) {
  const { data, error, count } = await query
  if (error) throw new Error(error.message)
  return count === null || count === undefined ? data : { rows: data, count }
}
