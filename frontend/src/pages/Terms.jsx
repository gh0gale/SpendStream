import { Link } from 'react-router'
import { usePageTitle } from '../lib/usePageTitle'
import { CONTACT_EMAIL, LEGAL_UPDATED, OPERATOR } from './legal'
import { Clause } from './Privacy'
import styles from './Reading.module.css'

export default function Terms() {
  usePageTitle('Terms of Service')

  return (
    <article className={`page ${styles.article} ${styles.legal}`}>
      <header className={styles.head}>
        <h1>Terms of Service</h1>
        <p className={styles.updated}>Last updated {LEGAL_UPDATED}</p>
        <p>
          These terms are an agreement between you and {OPERATOR}, who runs SpendStream. By creating
          an account you accept them.
        </p>
      </header>

      <Clause n="1" title="Who may use SpendStream">
        <p>
          You must be 18 or older, and you may connect only a Gmail account that is yours. One account
          is for one person.
        </p>
      </Clause>

      <Clause n="2" title="What SpendStream is">
        <p>
          SpendStream reads bank debit alerts in your Gmail and shows your spending by category. It is
          free. It is not a bank, does not move money, and gives no financial, tax or investment advice.
        </p>
      </Clause>

      <Clause n="3" title="Accuracy">
        <p>
          Payments are read from email and categorised automatically, and both steps can be wrong or
          incomplete: an alert may be missed or misread, and a category may be wrong. Only some banks'
          alert formats have been checked (see <Link to="/how-it-works">How it works</Link>). Check
          anything important against your bank statement.
        </p>
      </Clause>

      <Clause n="4" title="Acceptable use">
        <p>
          Do not try to access other people's data, disrupt the service, probe it for weaknesses
          without permission, or use it for anything unlawful. Accounts that do may be closed.
        </p>
      </Clause>

      <Clause n="5" title="Availability">
        <p>
          SpendStream runs on free hosting and may be slow to start, unavailable at times, changed or
          discontinued. If it is discontinued, we will give signed-in users notice where we can and
          delete their data.
        </p>
      </Clause>

      <Clause n="6" title="Ending your use">
        <p>
          You can delete your account at any time from the Account page; your data is deleted as
          described in the <Link to="/privacy">Privacy Policy</Link>. We may close an account that
          breaks these terms.
        </p>
      </Clause>

      <Clause n="7" title="Liability">
        <p>
          SpendStream is provided as is, without warranties. To the extent the law allows, {OPERATOR} is
          not liable for losses arising from its use, including decisions made from its figures.
          Nothing here limits rights you have under Indian consumer law.
        </p>
      </Clause>

      <Clause n="8" title="Law and contact">
        <p>
          These terms are governed by the laws of India. Questions: <a href={`mailto:${CONTACT_EMAIL}`}>{CONTACT_EMAIL}</a>.
        </p>
      </Clause>
    </article>
  )
}
