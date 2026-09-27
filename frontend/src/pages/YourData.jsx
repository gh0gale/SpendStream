import { useState } from 'react'
import { Link } from 'react-router'
import { usePageTitle } from '../lib/usePageTitle'
import { CONTACT_EMAIL, OPERATOR } from './legal'
import styles from './Reading.module.css'

// Must match BANK_QUERY in backend/tasks.py.
const GMAIL_QUERY = 'from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) (debited OR spent OR txn OR transaction)'

const KEPT = [
  ['Each debit alert', 'Amount, date and time, the payee as the bank wrote it, the first 200 characters of the alert, and the Gmail message id (so the same email is never stored twice).'],
  ['Each payment', 'The cleaned merchant name, the payee id used to recognise the merchant again, whether the payee is a person, and the category.'],
  ['Your corrections', 'Each category you chose, and the rule it created for that merchant.'],
  ['Your Gmail connection', 'Google\'s access token and refresh token (the refresh token is encrypted), when mail was last read, and whether Google needs you to reconnect.'],
  ['Your syncs', 'When each sync ran, whether it worked, and how many payments it found.'],
  ['Your account', 'Your email address, and your password as a hash kept by Supabase Auth. SpendStream never sees your password.'],
]

export default function YourData() {
  usePageTitle('What we read and keep')
  const [copied, setCopied] = useState(false)

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(GMAIL_QUERY)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      setCopied(false)
    }
  }

  return (
    <article className={`page ${styles.article}`}>
      <header className={styles.head}>
        <h1>What we read and keep</h1>
        <p className="lead">
          Read-only access to your Gmail. Only bank alerts are searched. Everything is stored so
          that only you can read it, and you can delete all of it at any time.
        </p>
      </header>

      <Section n="01" title="Exactly what we search for">
        <p>SpendStream asks Gmail for messages matching this search, and no others:</p>
        <pre className={styles.query}><code>{GMAIL_QUERY}</code></pre>
        <div className={styles.copyRow}>
          <button type="button" className="btn" onClick={copy}>Copy</button>
          <span className="small muted" aria-live="polite">{copied ? 'Copied' : ''}</span>
        </div>
        <p>
          Paste it into Gmail's search box to see the same messages SpendStream sees. A matching
          message that is not a debit alert, such as a credit or a notice, is read and then
          discarded. Google grants read-only access, so SpendStream cannot send, delete or change
          any email.
        </p>
      </Section>

      <Section n="02" title="What we keep">
        <table className="ledger">
          <thead><tr><th scope="col" className="caption">What</th><th scope="col" className="caption">Stored</th></tr></thead>
          <tbody>
            {KEPT.map(([what, stored]) => (
              <tr key={what}><th scope="row">{what}</th><td>{stored}</td></tr>
            ))}
          </tbody>
        </table>
      </Section>

      <Section n="03" title="Who can see it">
        <p>
          Only you. Every table is protected by row-level security in the database, so a signed-in
          person can read only their own rows. Your Gmail tokens sit in a table no browser can read
          at all. Payments are never shared with other users.
        </p>
      </Section>

      <Section n="04" title="No AI company">
        <p>
          Categories come from a small model that runs on SpendStream's own server. No transaction,
          merchant or email is sent to an outside AI service.
        </p>
      </Section>

      <Section n="05" title="Google's warning screen">
        <p>
          When you connect Gmail, Google says the app is not verified. Google has not reviewed
          SpendStream: verifying an app that reads Gmail needs a paid security assessment, which a
          small independent project has not done. Until an app is verified, Google lets at most
          100 people connect it. The warning is about that review. It does not mean something is
          wrong with your account.
        </p>
      </Section>

      <Section n="06" title="Leaving">
        <p>
          Account, then Delete account, removes every payment, rule, correction and sync record,
          deletes your Gmail tokens, asks Google to revoke SpendStream's access, and deletes your
          sign-in. You can also remove access yourself at any time in your{' '}
          <a href="https://myaccount.google.com/permissions">Google account's third-party access page</a>.
        </p>
      </Section>

      <Section n="07" title="Who runs this">
        <p>
          SpendStream is run by {OPERATOR}, an individual developer in India. Questions about your
          data: <a href={`mailto:${CONTACT_EMAIL}`}>{CONTACT_EMAIL}</a>. The formal version is the{' '}
          <Link to="/privacy">Privacy Policy</Link>.
        </p>
      </Section>
    </article>
  )
}

function Section({ n, title, children }) {
  return (
    <section className={styles.step} aria-labelledby={`s-${n}`}>
      <span className={styles.stepNumber} aria-hidden="true">{n}</span>
      <div className={styles.stepBody}>
        <h2 id={`s-${n}`}>{title}</h2>
        {children}
      </div>
    </section>
  )
}
