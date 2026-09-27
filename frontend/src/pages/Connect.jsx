import { useState } from 'react'
import { Link } from 'react-router'
import { WakingNote } from '../components/States'
import { apiFetch } from '../lib/api'
import { useBackendAction } from '../lib/useBackendAction'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Connect.module.css'

export default function Connect() {
  const [step, setStep] = useState(1)
  const [error, setError] = useState('')
  usePageTitle(step === 1 ? 'Connect Gmail' : 'Before Google')

  // The backend returns Google's consent URL; the session token never goes in a URL.
  const start = useBackendAction(() => apiFetch('/auth/google/start', { method: 'POST' }))

  const goToGoogle = async () => {
    setError('')
    try {
      const { url } = await start.run()
      window.location.assign(url)
    } catch (err) {
      setError(err.message)
    }
  }

  return (
    <div className={styles.wrap}>
      <Link to="/app" className={styles.wordmark}>SpendStream</Link>
      <div className={styles.column}>
        <p className="caption">Step {step} of 2</p>

        {step === 1 ? (
          <>
            <h1>Connect the Gmail that gets your bank alerts</h1>
            <table className="ledger">
              <thead>
                <tr><th scope="col" className="caption">Will read</th><th scope="col" className="caption">Will not read</th></tr>
              </thead>
              <tbody>
                <tr>
                  <td>Emails from HDFC, ICICI, SBI, Axis, Kotak or Yes Bank that mention a debit</td>
                  <td>Any other email</td>
                </tr>
                <tr>
                  <td>From each debit alert: amount, date, who was paid</td>
                  <td>Contacts, drafts, attachments. Nothing is ever sent, deleted or changed</td>
                </tr>
              </tbody>
            </table>
            <Link to="/your-data" className="link-arrow">Read exactly what we keep</Link>
            <div className={styles.actions}>
              <button type="button" className="btn btn-primary" onClick={() => setStep(2)}>Continue</button>
              <Link to="/app">Not now</Link>
            </div>
          </>
        ) : (
          <>
            <h1>Google will say this app is not verified</h1>
            <p>
              That is because SpendStream is a small independent project that Google has not reviewed
              (it is limited to 100 users), not because of anything wrong with your account.
            </p>
            <p>On Google's warning screen:</p>
            <ol className={styles.steps}>
              <li>Choose <strong>Advanced</strong>.</li>
              <li>Choose the link to go to SpendStream.</li>
              <li>Allow read-only access to your email.</li>
            </ol>
            {error && <p className="help-error" role="alert">{error}</p>}
            <div className={styles.actions}>
              <button type="button" className="btn btn-primary" onClick={goToGoogle} disabled={start.pending}>
                {start.pending && <span className="spinner" aria-hidden="true" />}
                {start.waking ? 'Starting server' : 'Continue to Google'}
              </button>
              <button type="button" className="btn btn-text" onClick={() => setStep(1)} disabled={start.pending}>Go back</button>
            </div>
            <WakingNote action={start} />
          </>
        )}
      </div>
    </div>
  )
}
