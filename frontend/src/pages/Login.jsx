import { useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router'
import { supabase } from '../lib/supabase'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Login.module.css'

const MIN_PASSWORD = 8

export default function Login() {
  const [params, setParams] = useSearchParams()
  const mode = params.get('mode') === 'signup' ? 'signup' : 'signin'
  const isSignup = mode === 'signup'
  usePageTitle(isSignup ? 'Create account' : 'Sign in')

  const navigate = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')

  const passwordShort = isSignup && password.length > 0 && password.length < MIN_PASSWORD
  const canSubmit = email.trim() && password && !passwordShort && !submitting

  const switchMode = (next) => {
    setParams(next === 'signup' ? { mode: 'signup' } : {}, { replace: true })
    setError('')
    setNotice('')
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')
    setNotice('')
    setSubmitting(true)
    try {
      if (isSignup) {
        const { data, error: err } = await supabase.auth.signUp({ email: email.trim(), password })
        if (err) throw err
        // With email confirmation off Supabase signs the user in at once.
        if (data.session) navigate('/connect', { replace: true })
        else setNotice('Account created. Check your email to confirm it, then sign in.')
      } else {
        const { error: err } = await supabase.auth.signInWithPassword({ email: email.trim(), password })
        if (err) throw err
        navigate('/app', { replace: true })
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setSubmitting(false)
    }
  }

  const handleGoogle = async () => {
    setError('')
    const { error: err } = await supabase.auth.signInWithOAuth({
      provider: 'google',
      options: { redirectTo: `${window.location.origin}/app` },
    })
    if (err) setError(err.message)
  }

  return (
    <div className={styles.wrap}>
      <aside className={`band ${styles.brand}`}>
        <Link to="/" className={styles.wordmark}><span className="brand-stamp" aria-hidden="true">₹</span>SpendStream</Link>
        <p className={`display ${styles.pitch}`}>Every UPI payment, sorted from the alerts your bank already sends.</p>
        <ul className={styles.points}>
          <li>Reads only bank debit alerts, with read-only Gmail access.</li>
          <li>Asks you when it is less than 50% sure.</li>
          <li>Delete your account and every row goes.</li>
        </ul>
      </aside>
      <div className={styles.side}>
        <div className={styles.panel}>
          <h1 className="visually-hidden">{isSignup ? 'Create account' : 'Sign in'}</h1>
          <div className={styles.toggle} role="radiogroup" aria-label="Choose">
            {[['signup', 'Create account'], ['signin', 'Sign in']].map(([m, label]) => (
              <button
                key={m}
                type="button"
                role="radio"
                aria-checked={mode === m}
                className={`${styles.toggleButton} ${mode === m ? styles.selected : ''}`}
                onClick={() => switchMode(m)}
              >
                {label}
              </button>
            ))}
          </div>

          <button type="button" className="btn btn-block" onClick={handleGoogle}>
            Continue with Google
          </button>
          <p className={styles.or}><span>or</span></p>

          <form className={styles.form} onSubmit={handleSubmit} noValidate>
            <div className="field">
              <label htmlFor="email">Email</label>
              <input id="email" className="input" type="email" autoComplete="email"
                value={email} onChange={e => setEmail(e.target.value)} required />
            </div>
            <div className="field">
              <label htmlFor="password">Password</label>
              <input id="password" className="input" type="password"
                autoComplete={isSignup ? 'new-password' : 'current-password'}
                aria-describedby={isSignup ? 'password-help' : undefined}
                aria-invalid={passwordShort}
                value={password} onChange={e => setPassword(e.target.value)} required />
              {isSignup && (
                <p id="password-help" className={passwordShort ? 'help-error' : 'help'}>
                  At least {MIN_PASSWORD} characters
                </p>
              )}
            </div>

            {error && <p className="help-error" role="alert">{error}</p>}
            {notice && <p role="status">{notice}</p>}

            <button type="submit" className="btn btn-primary btn-block" disabled={!canSubmit}>
              {submitting && <span className="spinner" aria-hidden="true" />}
              {submitting
                ? (isSignup ? 'Creating account' : 'Signing in')
                : (isSignup ? 'Create account' : 'Sign in')}
            </button>
          </form>

          {isSignup && (
            <p className="small muted">
              Creating an account does not give us your Gmail. You connect that in the next step.
            </p>
          )}
        </div>
        <p className={`small ${styles.links}`}>
          <Link to="/privacy">Privacy</Link>
          <Link to="/terms">Terms</Link>
        </p>
      </div>
    </div>
  )
}
