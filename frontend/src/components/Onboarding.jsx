import { Link } from 'react-router'
import styles from './Onboarding.module.css'

// The one thing a first-time user gets wrong: connecting a Gmail that never
// receives their bank's alerts. Shown on the welcome screen and on /connect.
export function WhichGmail() {
  return (
    <div className="notice">
      <p className="notice-title">Connect the Gmail your bank sends alerts to</p>
      <p>
        That is the email address registered with your bank account. It can be a different Google
        account from the one you signed in with: Google will ask which account to use, so pick that one.
      </p>
      <p className="small muted">
        Not sure which? Search each Gmail for <strong>debited</strong>. The one with emails like
        "Rs.250 has been debited from your account" is right. No such emails anywhere? Turn on email
        alerts for transactions in your bank's app or net banking first.
      </p>
    </div>
  )
}

// First visit, Gmail not connected yet: what SpendStream does and the first step.
export function Welcome() {
  return (
    <section className={styles.welcome} aria-labelledby="welcome-h">
      <header className="page-head">
        <p className="caption">Welcome</p>
        <h1 id="welcome-h" className="display">Let's find your <span className="mark">bank alerts</span>.</h1>
        <p className="lead">
          SpendStream builds your spending dashboard from the debit alerts your bank already emails
          you. No bank login, nothing to type in.
        </p>
      </header>

      <WhichGmail />

      <ol className={styles.steps}>
        <li>
          <span className={styles.num} aria-hidden="true">1</span>
          <div>
            <h2>Connect Gmail</h2>
            <p className="muted">Read-only. SpendStream searches only for bank alerts and never sends, deletes or changes an email.</p>
          </div>
        </li>
        <li>
          <span className={styles.num} aria-hidden="true">2</span>
          <div>
            <h2>We read this month</h2>
            <p className="muted">Each debit alert becomes a payment: amount, date, who was paid, and a category. You can go back up to 12 months later.</p>
          </div>
        </li>
        <li>
          <span className={styles.num} aria-hidden="true">3</span>
          <div>
            <h2>You settle the unsure ones</h2>
            <p className="muted">Payments SpendStream cannot place wait in Needs review. Correct one and every payment to that merchant follows.</p>
          </div>
        </li>
      </ol>

      <div className={styles.actions}>
        <Link to="/connect" className="btn btn-primary">Connect Gmail</Link>
        <Link to="/your-data">What exactly is read</Link>
      </div>
    </section>
  )
}

// After the first payments arrive: how to use the app, until the user hides it.
export function GettingStarted({ reviewCount, onHide }) {
  return (
    <section className={`card ${styles.guide}`} aria-labelledby="guide-h">
      <div className="card-head">
        <h2 id="guide-h">Getting started</h2>
        <button type="button" className="btn btn-text" onClick={onHide}>Hide this guide</button>
      </div>
      <ol className={styles.tips}>
        <li>
          <h3>Settle the unsure payments</h3>
          <p className="small muted">
            {reviewCount > 0
              ? <><Link to="/app/review">Needs review</Link> has payments SpendStream could not place. </>
              : <>Payments SpendStream cannot place go to <Link to="/app/review">Needs review</Link>. </>}
            Correct one and every payment to that merchant moves with it.
          </p>
        </li>
        <li>
          <h3>Change any category</h3>
          <p className="small muted">
            In <Link to="/app/transactions">Transactions</Link>, press a payment's category. Your change
            becomes a rule for that merchant; <Link to="/app/rules">see your rules</Link>.
          </p>
        </li>
        <li>
          <h3>It keeps itself up to date</h3>
          <p className="small muted">
            SpendStream checks your Gmail three times a day. Sync now fetches new alerts straight away.
          </p>
        </li>
        <li>
          <h3>Go further back</h3>
          <p className="small muted">
            The Read button next to Sync now reads one earlier month per press, up to 12 months back.
          </p>
        </li>
        <li>
          <h3>Search and export</h3>
          <p className="small muted">
            Transactions filters by month, category and merchant, and downloads what you see as CSV.
          </p>
        </li>
        <li>
          <h3>Payments missing?</h3>
          <p className="small muted">
            Only debit alerts from the connected Gmail are read. If your bank emails another address,{' '}
            <Link to="/connect">connect that Gmail</Link> instead.
          </p>
        </li>
      </ol>
    </section>
  )
}
