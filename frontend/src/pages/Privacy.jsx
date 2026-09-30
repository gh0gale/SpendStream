import { Link } from 'react-router'
import { usePageTitle } from '../lib/usePageTitle'
import { CONTACT_EMAIL, LEGAL_UPDATED, OPERATOR } from './legal'
import styles from './Reading.module.css'

const mail = <a href={`mailto:${CONTACT_EMAIL}`}>{CONTACT_EMAIL}</a>

export default function Privacy() {
  usePageTitle('Privacy Policy')

  return (
    <article className={`page ${styles.article} ${styles.legal}`}>
      <header className={styles.head}>
        <h1>Privacy Policy</h1>
        <p className={styles.updated}>Last updated {LEGAL_UPDATED}</p>
        <p>
          This policy explains what personal data SpendStream processes, why, and your rights. It is
          the notice required by India's Digital Personal Data Protection Act, 2023. The plain-language
          version is <Link to="/your-data">What we read and keep</Link>.
        </p>
      </header>

      <Clause n="1" title="Who is responsible">
        <p>
          SpendStream is run by {OPERATOR}, an individual in India, who is the data fiduciary for your
          personal data. Contact for every privacy question, request or grievance: {mail}.
        </p>
      </Clause>

      <Clause n="2" title="What we collect and why">
        <ul>
          <li><strong>Account data:</strong> your email address, to sign you in. Passwords are handled by our authentication provider and are never visible to us.</li>
          <li><strong>Bank alert emails:</strong> with your consent, read-only access to your Gmail, used only to search for bank debit alerts. From each debit alert we store the amount, date and time, the payee as written, the first 200 characters of the alert and the Gmail message id. Matching emails that are not debit alerts are discarded.</li>
          <li><strong>Derived data:</strong> a cleaned merchant name, a merchant identifier, whether the payee is a person, and a spending category for each payment.</li>
          <li><strong>Your corrections:</strong> the categories you choose, stored as rules that apply to your own payments.</li>
          <li><strong>Connection and sync records:</strong> Google access and refresh tokens (the refresh token encrypted), and the time, outcome and email counts of each sync.</li>
          <li><strong>Usage records:</strong> which screens and actions you use (for example that you opened the dashboard or corrected a category), with counts such as how many months were shown, never merchant names, amounts or email text. They are kept for 90 days and used only to see whether the product works.</li>
        </ul>
        <p>
          The only purpose is to show you your own spending by category. We do not use your data for
          advertising, do not sell it, and do not use it to build profiles for anyone else.
        </p>
      </Clause>

      <Clause n="3" title="Google user data">
        <blockquote className={styles.blockquote}>
          SpendStream's use and transfer to any other app of information received from Google APIs
          will adhere to the{' '}
          <a href="https://developers.google.com/terms/api-services-user-data-policy">Google API Services User Data Policy</a>,
          including the Limited Use requirements.
        </blockquote>
        <p>
          Gmail data is used only to provide the spending features you see. No person reads your
          emails, except with your explicit permission for a specific support request, for security
          purposes, or where the law requires it. Gmail data is never used to train a model that
          serves other users, and never transferred to others except as needed to run the service
          (section 5) or as the law requires.
        </p>
      </Clause>

      <Clause n="4" title="Consent and withdrawing it">
        <p>
          You give consent by connecting Gmail on the Connect Gmail screen, after it shows you what is
          read. You can withdraw consent at any time by deleting your account in the app, or by
          removing SpendStream in your Google account's third-party access settings. Withdrawing stops
          all further reading of your email.
        </p>
      </Clause>

      <Clause n="5" title="Where data is stored and who processes it">
        <p>
          Data is stored in a Postgres database run by Supabase. The service also uses a hosting
          provider for the server that reads your alerts and for the website, and Google for Gmail
          access and optional Google sign-in. These providers process data only to run SpendStream.
          No transaction data is sent to any AI provider.
        </p>
      </Clause>

      <Clause n="6" title="How long we keep it">
        <p>
          We keep your data while your account exists. When you delete your account, your payments,
          rules, corrections, sync records, usage records, Gmail tokens and sign-in are deleted at once, and Google is
          asked to revoke access. Copies may remain in the database provider's backups until those
          backups expire.
        </p>
      </Clause>

      <Clause n="7" title="Your rights">
        <ul>
          <li>Access a summary of the personal data we hold about you.</li>
          <li>Correct it: categories can be changed in the app; for anything else, write to us.</li>
          <li>Erase it: delete your account in the app, or ask us to.</li>
          <li>Nominate a person to exercise these rights if you die or become incapable.</li>
          <li>Raise a grievance with us at {mail}. We will reply within 30 days. If you are not satisfied, you may complain to the Data Protection Board of India.</li>
        </ul>
      </Clause>

      <Clause n="8" title="Security and breaches">
        <p>
          Every table is protected by row-level security so each person can read only their own rows,
          Gmail refresh tokens are encrypted, and the browser never holds privileged keys. If a
          personal data breach happens, we will inform affected users and the Data Protection Board
          of India as the law requires.
        </p>
      </Clause>

      <Clause n="9" title="Children">
        <p>SpendStream is for people aged 18 and over. Do not create an account if you are younger.</p>
      </Clause>

      <Clause n="10" title="Changes to this policy">
        <p>
          We will change the date at the top when this policy changes, and tell signed-in users in
          the app before a change that affects how their data is used.
        </p>
      </Clause>
    </article>
  )
}

export function Clause({ n, title, children }) {
  return (
    <section className={styles.step} aria-labelledby={`c-${n}`}>
      <span className={styles.stepNumber} aria-hidden="true">{n}</span>
      <div className={styles.stepBody}>
        <h2 id={`c-${n}`}>{title}</h2>
        {children}
      </div>
    </section>
  )
}
