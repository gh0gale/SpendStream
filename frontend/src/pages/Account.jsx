import { useState } from 'react'
import { Link, useNavigate } from 'react-router'
import { ErrorNotice, SkeletonBlock, WakingNote } from '../components/States'
import { apiFetch } from '../lib/api'
import { useAuth } from '../lib/auth'
import { formatDateTime } from '../lib/format'
import { supabase } from '../lib/supabase'
import { useBackendAction } from '../lib/useBackendAction'
import { useGmailSync } from '../lib/useGmailSync'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Account.module.css'

export default function Account() {
  usePageTitle('Account')
  const { user } = useAuth()
  const navigate = useNavigate()
  const { gmail, reloadGmail } = useGmailSync(user.id)

  const signOut = async () => {
    await supabase.auth.signOut()
    navigate('/', { replace: true })
  }

  return (
    <div className={styles.page}>
      <header className="page-head">
        <p className="caption">Settings</p>
        <h1 className="display">Account</h1>
      </header>

      <div className={styles.sheet}>
        <section className={styles.section} aria-labelledby="gmail-h">
          <h2 id="gmail-h" className="caption">Gmail</h2>
          <div className={styles.body}><GmailStatus gmail={gmail} onRetry={reloadGmail} /></div>
        </section>

        <section className={styles.section} aria-labelledby="rules-h">
          <h2 id="rules-h" className="caption">Category rules</h2>
          <div className={`${styles.body} ${styles.row}`}>
            <p>Every category you chose is remembered for that merchant.</p>
            <Link to="/app/rules" className="btn">See your rules</Link>
          </div>
        </section>

        <section className={styles.section} aria-labelledby="account-h">
          <h2 id="account-h" className="caption">Signed in</h2>
          <div className={`${styles.body} ${styles.row}`}>
            <p><strong>{user.email}</strong></p>
            <button type="button" className="btn" onClick={signOut}>Sign out</button>
          </div>
        </section>

        <DeleteAccount email={user.email} />
      </div>
    </div>
  )
}

function GmailStatus({ gmail, onRetry }) {
  if (gmail.status === 'loading') return <SkeletonBlock height={44} width={320} />
  if (gmail.status === 'error') {
    return <ErrorNotice title="Couldn't check your Gmail connection." detail={gmail.detail} onRetry={onRetry} />
  }
  if (gmail.status === 'none') {
    return (
      <div className={styles.row}>
        <p>Gmail is not connected.</p>
        <Link to="/connect" className="btn btn-primary">Connect Gmail</Link>
      </div>
    )
  }
  return (
    <div className={styles.stack}>
      {gmail.status === 'reconnect' && (
        <p className={styles.attention} role="status">Gmail access has expired. Reconnect Gmail to keep syncing.</p>
      )}
      <div className={styles.row}>
        <p>
          {gmail.status === 'connected' ? 'Connected. ' : ''}
          {gmail.lastSynced ? <>Last synced <span className="num">{formatDateTime(gmail.lastSynced)}</span></> : 'Not synced yet'}
        </p>
        <Link to="/connect" className={`btn ${gmail.status === 'reconnect' ? 'btn-primary' : ''}`}>Reconnect Gmail</Link>
      </div>
    </div>
  )
}

function DeleteAccount({ email }) {
  const [open, setOpen] = useState(false)
  const [typed, setTyped] = useState('')
  const [error, setError] = useState('')
  const navigate = useNavigate()
  const remove = useBackendAction(() => apiFetch('/account', { method: 'DELETE' }))

  const matches = typed.trim().toLowerCase() === (email || '').toLowerCase()

  const handleDelete = async (e) => {
    e.preventDefault()
    if (!matches) return
    setError('')
    try {
      await remove.run()
      await supabase.auth.signOut()
      navigate('/deleted', { replace: true })
    } catch (err) {
      setError(err.message || 'Your account could not be deleted. Try again.')
    }
  }

  return (
    <section className={`${styles.section} ${styles.danger}`} aria-labelledby="delete-h">
      <h2 id="delete-h" className="caption">Delete account</h2>
      <div className={styles.body}>
        <p>
          Deleting removes every payment, every category rule and your sync history, deletes your Gmail
          connection, and asks Google to revoke SpendStream's access. It cannot be undone.{' '}
          <Link to="/privacy">Privacy Policy</Link>
        </p>
        {!open ? (
          <button type="button" className="btn btn-danger" onClick={() => setOpen(true)}>Delete my account</button>
        ) : (
          <form className={styles.confirm} onSubmit={handleDelete}>
            <div className="field">
              <label htmlFor="confirm-email">Type your email to confirm</label>
              <input id="confirm-email" className="input" type="email" autoComplete="off" autoFocus
                value={typed} onChange={e => setTyped(e.target.value)} aria-describedby="confirm-help" />
              <p id="confirm-help" className="help">
                {matches ? 'This will delete everything.' : `Type ${email} to enable the button.`}
              </p>
            </div>
            {error && <p className="help-error" role="alert">{error}</p>}
            <div className={styles.row}>
              <button type="submit" className="btn btn-danger" disabled={!matches || remove.pending}>
                {remove.pending && <span className="spinner" aria-hidden="true" />}
                {remove.waking ? 'Starting server' : remove.pending ? 'Deleting' : 'Delete everything'}
              </button>
              <button type="button" className="btn btn-text" disabled={remove.pending}
                onClick={() => { setOpen(false); setTyped(''); setError('') }}>Cancel</button>
            </div>
            <WakingNote action={remove} />
          </form>
        )}
      </div>
    </section>
  )
}
