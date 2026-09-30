import { supabase } from './supabase'

// First-party usage events (docs/scalability/02-product.md section 8). They go
// to log_event() in Postgres, which only accepts the allow-listed names and
// small props, and only from a signed-in user. props carry counts and short
// labels, never merchant names, amounts or email text. Fire and forget: a
// failed event must never reach the user.
export function track(name, props = {}) {
  supabase.rpc('log_event', { p_name: name, p_props: props }).then(() => {}, () => {})
}
