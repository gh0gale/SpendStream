import { Link } from 'react-router'
import CategoryChip from '../components/CategoryChip'
import { CATEGORIES, CATEGORY_HINTS } from '../lib/categories'
import { demo } from '../lib/demo'
import { usePageTitle } from '../lib/usePageTitle'
import { CorrectionDemo } from './Home'
import styles from './Reading.module.css'

const BANKS = [
  ['HDFC Bank', 'Checked on real alerts'],
  ['Axis Bank', 'Searched, not yet verified'],
  ['ICICI Bank', 'Searched, not yet verified'],
  ['Kotak Mahindra Bank', 'Searched, not yet verified'],
  ['State Bank of India', 'Searched, not yet verified'],
  ['Yes Bank', 'Searched, not yet verified'],
  ['IDFC FIRST Bank', 'Searched, not yet verified'],
  ['IndusInd Bank', 'Searched, not yet verified'],
  ['Federal Bank', 'Searched, not yet verified'],
  ['Bank of Baroda', 'Searched, not yet verified'],
  ['Punjab National Bank', 'Searched, not yet verified'],
  ['Canara Bank', 'Searched, not yet verified'],
  ['Union Bank of India', 'Searched, not yet verified'],
  ['RBL Bank', 'Searched, not yet verified'],
  ['AU Small Finance Bank', 'Searched, not yet verified'],
  ['IDBI Bank', 'Searched, not yet verified'],
]

export default function HowItWorks() {
  usePageTitle('How it works')

  return (
    <article className={`page ${styles.article}`}>
      <header className={styles.head}>
        <h1>How SpendStream knows what you spent on</h1>
        <p className="lead">
          Your bank's alert emails go in; a list of payments, each with a category, comes out.
          Nothing is typed by hand. Here is every step, in the order it happens.
        </p>
      </header>

      <Step n="01" title="Reading your alerts">
        <p>
          SpendStream searches your Gmail for mail from the banks below that mentions a debit. It
          syncs when you press Sync now, at most once every 10 minutes, and on a schedule three
          times a day once the service is live. The first sync reads this month so far. Each email is
          stored once, by its Gmail message id, so the same payment is never counted twice.
        </p>
        <table className="ledger">
          <caption className="caption">Banks searched</caption>
          <thead><tr><th scope="col" className="caption">Bank</th><th scope="col" className="caption">Status</th></tr></thead>
          <tbody>
            {BANKS.map(([bank, status]) => (
              <tr key={bank}><th scope="row">{bank}</th><td className={status.startsWith('Checked') ? '' : 'muted'}>{status}</td></tr>
            ))}
          </tbody>
        </table>
        <p className="small muted">
          Only debits are read. Credit alerts and account notices are left out on purpose. Credit
          card alerts have not been checked yet.
        </p>
      </Step>

      <Step n="02" title="Cleaning the merchant">
        <p>
          Alerts name the payee the way UPI does: a handle such as <code>swiggy@axisbank</code>, often
          with extra words. SpendStream strips the handle and the noise to get a name you recognise,
          and keeps the payee id underneath as the merchant's identity. It also tells a payment to a
          person from a payment to a shop, including shops paid through a QR code.
        </p>
        {demo?.cleaning?.length > 0 && (
          <table className="ledger">
            <caption className="caption">From the developer's own alerts</caption>
            <thead><tr><th scope="col" className="caption">As the bank wrote it</th><th scope="col" className="caption">Shown as</th></tr></thead>
            <tbody>
              {demo.cleaning.map(c => (
                <tr key={c.raw}>
                  <td><code>{c.raw}</code><span className="visually-hidden"> becomes</span></td>
                  <th scope="row">{c.clean}</th>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <p>A payment to a person is marked "to a person" in your list.</p>
      </Step>

      <Step n="03" title="Choosing a category, strongest first">
        <ol className={styles.ordered}>
          <li><strong>Your rule for this exact merchant.</strong> Once you have corrected a merchant, your answer is used and nothing else is asked.</li>
          <li><strong>A small model</strong> that reads the merchant's name, the amount and the time of the payment.</li>
          <li><strong>If the model is less than 50% sure,</strong> the payment is left as Unsure and waits for you in Needs review.</li>
        </ol>
      </Step>

      <Step n="04" title="Your corrections are yours alone">
        <p>
          A correction becomes a rule for you and that merchant. It moves your other payments to the
          same merchant, except any you corrected by hand, and sorts future ones. It never changes the
          model, and nobody else's payments are affected.
        </p>
        {demo?.correction && <CorrectionDemo correction={demo.correction} />}
      </Step>

      <Step n="05" title="The 13 categories">
        <dl className={styles.categories}>
          {CATEGORIES.map(c => (
            <div key={c}>
              <dt><CategoryChip category={c} /></dt>
              {CATEGORY_HINTS[c] && <dd className="small muted">{CATEGORY_HINTS[c]}</dd>}
            </div>
          ))}
          <div>
            <dt><CategoryChip category={null} /></dt>
            <dd className="small muted">Not a category: the model was not sure. Your pick settles it.</dd>
          </div>
        </dl>
      </Step>

      <Step n="06" title="How accuracy is measured" id="accuracy">
        <p>
          The model was scored on 312 real payments, labelled by hand, that it never saw during
          training. It chose the right category for 87.8% of them; the 95% range is 84.3% to 91.3%.
          Its macro-F1 is 0.745, which means some categories are harder than others. All 312 come
          from one person's payments, so your results may differ. The model used until 27 September
          2026 scored 38.6% on an earlier set of 132 payments.
        </p>
      </Step>

      <Step n="07" title="Limits">
        <p>
          Debits only. Verified on HDFC alerts only. Refunds are not netted off. Credit card alerts
          are not verified. One person per account. See <Link to="/your-data">what we read and keep</Link>.
        </p>
      </Step>
    </article>
  )
}

function Step({ n, title, id, children }) {
  return (
    <section className={styles.step} id={id} aria-labelledby={`step-${n}`}>
      <span className={styles.stepNumber} aria-hidden="true">{n}</span>
      <div className={styles.stepBody}>
        <h2 id={`step-${n}`}>{title}</h2>
        {children}
      </div>
    </section>
  )
}
