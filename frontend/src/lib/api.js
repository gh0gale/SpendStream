import { supabase } from './supabase'

// The one place the frontend reaches the backend. Reads the base URL from
// VITE_API_URL and sends the user's session token as a Bearer header.
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export async function apiFetch(path, { method = 'GET', body } = {}) {
  const { data } = await supabase.auth.getSession()
  const token = data.session?.access_token
  if (!token) throw new Error('Your session has expired. Sign in again.')

  const res = await fetch(`${API_URL}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  })

  const payload = await res.json().catch(() => ({}))
  if (!res.ok) {
    const error = new Error(payload.detail || `Request failed (${res.status})`)
    error.status = res.status
    throw error
  }
  return payload
}
