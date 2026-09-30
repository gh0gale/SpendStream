import { useCallback, useEffect, useState } from 'react'
import { apiFetch } from './api'
import { supabase } from './supabase'
import { track } from './track'
import { useBackendAction } from './useBackendAction'
import { must, useLoad } from './useLoad'

// A sync is a sync_jobs row; the dashboard reads it until it finishes.
// Progress is only ever the row's real status, never a timer.
const JOB_POLL_MS = 2000
const JOB_MAX_POLLS = 150              // 5 minutes
const JOB_RESUME_AGE_MS = 60 * 60 * 1000
export const MANUAL_SYNC_INTERVAL_MS = 10 * 60 * 1000   // main.MANUAL_SYNC_INTERVAL
const HISTORY_MONTHS = 12                              // tasks.HISTORY_MONTHS
const INDIA_OFFSET_MS = 5.5 * 60 * 60 * 1000           // India has no daylight saving

// gmail.status: 'loading' | 'error' | 'none' | 'connected' | 'reconnect'
// sync: null | { state: 'running' | 'succeeded' | 'failed' | 'timeout', found, error }
export function useGmailSync(userId, { onFinished } = {}) {
  const [jobId, setJobId] = useState(null)
  const [sync, setSync] = useState(null)

  // "Last synced" is the latest sync job that succeeded. gmail_sync.last_fetched
  // is the Gmail read cursor, which the backend holds back when a message
  // fails to download (DATA-07), so it can stay empty after good syncs.
  const fetchGmail = useCallback(async () => {
    const [data, lastJob] = await Promise.all([
      must(supabase.from('gmail_sync')
        .select('needs_reconnect, last_sync_requested_at, history_from')
        .eq('user_id', userId).maybeSingle()),
      must(supabase.from('sync_jobs')
        .select('finished_at').eq('user_id', userId).eq('status', 'succeeded')
        .order('finished_at', { ascending: false }).limit(1).maybeSingle()),
    ])
    if (!data) return { status: 'none' }
    return {
      status: data.needs_reconnect ? 'reconnect' : 'connected',
      lastSynced: lastJob?.finished_at ?? null,
      lastRequested: data.last_sync_requested_at,
      historyFrom: data.history_from,
    }
  }, [userId])
  const gmailLoad = useLoad(fetchGmail)
  const loadGmail = gmailLoad.reload
  // While a refresh is open the last known state stays on screen.
  const gmail = gmailLoad.status === 'error'
    ? { status: 'error', detail: gmailLoad.detail }
    : gmailLoad.data ?? { status: 'loading' }

  // A sync started before a reload is picked up again.
  useEffect(() => {
    let cancelled = false
    supabase.from('sync_jobs').select('id')
      .eq('user_id', userId).in('status', ['queued', 'running'])
      .gte('created_at', new Date(Date.now() - JOB_RESUME_AGE_MS).toISOString())
      .order('created_at', { ascending: false }).limit(1)
      .then(({ data }) => {
        if (!cancelled && data?.length) {
          setJobId(data[0].id)
          setSync({ state: 'running' })
        }
      })
    return () => { cancelled = true }
  }, [userId])

  useEffect(() => {
    if (!jobId) return
    let done = false
    let polls = 0
    const timer = setInterval(async () => {
      const { data: job, error } = await supabase.from('sync_jobs')
        .select('status, error, transactions_found, messages_failed').eq('id', jobId).maybeSingle()
      if (done) return
      polls += 1
      if (!error && job?.status === 'succeeded') {
        done = true
        setJobId(null)
        setSync({ state: 'succeeded', found: job.transactions_found ?? 0, retrying: job.messages_failed ?? 0 })
        track('sync_finished', { state: 'succeeded', found: job.transactions_found ?? 0 })
        loadGmail()
        onFinished?.()
      } else if (!error && job?.status === 'failed') {
        done = true
        setJobId(null)
        setSync({ state: 'failed', error: job.error || 'Gmail sync failed. Try again later.' })
        track('sync_finished', { state: 'failed' })
        loadGmail()
      } else if (polls >= JOB_MAX_POLLS) {
        // The polling window ended with the job still running: say so honestly.
        done = true
        setJobId(null)
        setSync({ state: 'timeout', jobId })
      }
    }, JOB_POLL_MS)
    return () => { done = true; clearInterval(timer) }
  }, [jobId, loadGmail, onFinished])

  const startAction = useBackendAction(() => apiFetch('/fetch-gmail'))
  const backfillAction = useBackendAction(() => apiFetch('/backfill-gmail', { method: 'POST' }))
  const { run: runStart } = startAction
  const { run: runBackfill } = backfillAction

  // Both a normal sync and "read an earlier month" end in a sync_jobs row the
  // page polls; the same errors apply (409, 429).
  const begin = useCallback(async (run) => {
    setSync(null)
    try {
      const { job_id: id } = await run()
      setJobId(id)
      setSync({ state: 'running' })
      loadGmail()
    } catch (err) {
      // 409: not connected or reconnect needed; 429: synced in the last 10 minutes.
      setSync({ state: 'failed', error: err.message, status: err.status })
      if (err.status === 409 || err.status === 429) loadGmail()
      return err.status
    }
  }, [loadGmail])
  const start = useCallback(() => begin(runStart), [begin, runStart])
  const startBackfill = useCallback(() => begin(runBackfill), [begin, runBackfill])

  const checkAgain = useCallback(() => {
    if (sync?.jobId) {
      setJobId(sync.jobId)
      setSync({ state: 'running' })
    }
  }, [sync])

  return { gmail, sync, start, startAction, startBackfill, backfillAction, checkAgain, reloadGmail: loadGmail }
}

// The month "Read an earlier month" reads next, as 'YYYY-MM-01' (India's
// calendar, like the backend). '' when the oldest month read is not recorded
// yet (the backend then works it out); null once it would be more than
// HISTORY_MONTHS before the current month.
export function nextHistoryMonth(historyFrom, now = new Date()) {
  if (!historyFrom) return ''
  const oldest = new Date(new Date(historyFrom).getTime() + INDIA_OFFSET_MS)   // UTC fields read as India time
  const prev = new Date(Date.UTC(oldest.getUTCFullYear(), oldest.getUTCMonth() - 1, 1))
  const today = new Date(now.getTime() + INDIA_OFFSET_MS)
  const prevIndex = prev.getUTCFullYear() * 12 + prev.getUTCMonth()
  if (prevIndex < today.getUTCFullYear() * 12 + today.getUTCMonth() - HISTORY_MONTHS) return null
  return `${prev.getUTCFullYear()}-${String(prev.getUTCMonth() + 1).padStart(2, '0')}-01`
}

// Minutes until another manual sync is allowed, or 0.
export function minutesUntilSyncAllowed(lastRequested, now) {
  if (!lastRequested) return 0
  // The request time comes from the server's clock; a server running ahead
  // of this device would otherwise show more than the 10-minute limit.
  const wait = Math.min(new Date(lastRequested).getTime() + MANUAL_SYNC_INTERVAL_MS - now, MANUAL_SYNC_INTERVAL_MS)
  return wait > 0 ? Math.ceil(wait / 60000) : 0
}
