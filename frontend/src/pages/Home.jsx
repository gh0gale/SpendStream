import { useState } from 'react'
import { Link } from 'react-router'
import CategoryBreakdown from '../components/CategoryBreakdown'
import CategoryChip from '../components/CategoryChip'
import CategoryPicker from '../components/CategoryPicker'
import ResultNotice from '../components/ResultNotice'
import StampSorter from '../components/StampSorter'
import { useAuth } from '../lib/auth'
import { demo } from '../lib/demo'
import { formatDay, formatINR, formatMonth, plural } from '../lib/format'
import { useInView } from '../lib/motion'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Home.module.css'

const FACTS = [
  ['Read-only', 'Google lets SpendStream read, never send, delete or change mail.'],
  ['Bank alerts only', 'The Gmail search matches debit alerts from six banks. Nothing else is opened.'],
  ['No AI company', 'Payments are sorted on SpendStream’s own server.'],
  ['Gone in one step', 'Delete your account and every row goes, and Google access is revoked.'],
]

const FAQ = [
  ['Which banks work?',
    'HDFC alerts are checked against real mail. ICICI, SBI, Axis, Kotak and Yes Bank alerts are searched but not yet verified, so some may be missed. Credit card alerts are not verified yet.'],
  ['Does it track money coming in?',
    'No. Only debits are read. Credits, refunds and balance alerts are left out.'],
  ['What if a category is wrong?',
    'Press it and pick the right one. That becomes your rule for that merchant, and your other payments to it move at once.'],
  ['Why does Google warn me when I connect?',
    'Google has not reviewed SpendStream: that needs a paid security assessment, and until then an app can have at most 100 users. The warning is about that review, not about your account.'],
  ['What does it cost?', 'Nothing. SpendStream is free.'],
]

export default function Home() {
  usePageTitle(null)
  const { user } = useAuth()
  const cta = user
    ? <Link to="/app" className="btn btn-primary">Open your dashboard</Link>
    : <Link to="/login?mode=signup" className="btn btn-primary">Create a free account</Link>
  const earlier = demo?.correction && { ...demo.correction.row, category: demo.correction.chosen }

  return (
    <>
      <section className={`page ${styles.hero}`}>
        <div className={styles.heroText}>
          <p className={styles.tag}><span>Free</span><span>For UPI</span><span>Verified on HDFC alerts</span></p>
          <h1 className={`display ${styles.title}`}>UPI spending, <span className="mark">sorted</span> on its own.</h1>
          <p className="lead">
            Your bank already emails you after every payment. SpendStream reads only those alerts,
            files each payment under one of 13 categories, and asks you when it isn&apos;t sure.
          </p>
          <div className={styles.ctas}>
            {cta}
            <Link to="/your-data" className="link-arrow">What we read from your Gmail</Link>
          </div>
        </div>
        {demo?.alert && <div className={styles.heroArt}><StampSorter alert={demo.alert} earlier={earlier} /></div>}
      </section>

      <section className={styles.facts} aria-label="What SpendStream does with your Gmail">
        <ul className="page">
          {FACTS.map(([title, text]) => (
            <li key={title}><strong>{title}</strong><span>{text}</span></li>
          ))}
        </ul>
      </section>

      <section className={`page ${styles.section}`} aria-labelledby="steps-h">
        <div className={styles.sectionHead}>
          <p className="caption">How it works</p>
          <h2 id="steps-h" className="display">From alert to answer in <span className="mark">four steps</span>.</h2>
        </div>
        <ol className={styles.steps}>
          {steps().map((s, i) => (
            <li key={s.title} className={styles.step}>
              <span className={styles.stepNumber} aria-hidden="true">{i + 1}</span>
              <div className={styles.stepText}>
                <h3>{s.title}</h3>
                <p className="muted">{s.body}</p>
              </div>
              {s.art && <div className={styles.stepArt}>{s.art}</div>}
            </li>
          ))}
        </ol>
      </section>

      {demo && <MonthBand />}

      <section className={`page ${styles.section} ${styles.split}`} aria-labelledby="unsure-h">
        <div className={styles.sectionHead}>
          <p className="caption">Honest about mistakes</p>
          <h2 id="unsure-h" className="display">When it isn&apos;t sure, it <span className="mark">says so</span>.</h2>
          <p className="muted">
            Anything the model is less than 50% sure about is left as Unsure and waits for you in
            Needs review, instead of hiding in Other. It still counts in your monthly total.
          </p>
          <Link to="/how-it-works#accuracy" className="link-arrow">How we measure accuracy</Link>
        </div>
        <Receipt />
      </section>

      <section className={`page ${styles.section} ${styles.split}`} aria-labelledby="data-h">
        <div className={styles.sectionHead}>
          <p className="caption">Your data</p>
          <h2 id="data-h" className="display">We read your bank alerts. <span className="mark">Nothing else.</span></h2>
          <Link to="/your-data" className="link-arrow">Read exactly what we keep</Link>
        </div>
        <table className={`ledger ${styles.readTable}`}>
          <thead><tr><th scope="col" className="caption">We read</th><th scope="col" className="caption">We never read</th></tr></thead>
          <tbody>
            <tr><td>Debit alerts from HDFC, ICICI, SBI, Axis, Kotak and Yes Bank</td><td>Any other email</td></tr>
            <tr><td>From each alert: amount, date, who was paid</td><td>Contacts, drafts, attachments</td></tr>
            <tr><td>Your own corrections</td><td>Your bank login or statements</td></tr>
          </tbody>
        </table>
      </section>

      <section className={`page ${styles.section}`} aria-labelledby="faq-h">
        <div className={styles.sectionHead}>
          <p className="caption">Questions</p>
          <h2 id="faq-h" className="display">Before you connect.</h2>
        </div>
        <div className={styles.faq}>
          {FAQ.map(([q, a]) => (
            <details key={q}>
              <summary>{q}</summary>
              <p className="muted">{a}</p>
            </details>
          ))}
        </div>
      </section>

      <section className={`page ${styles.closing}`}>
        <h2 className="display">Know where <span className="mark">every rupee</span> went.</h2>
        <div className={styles.closingAction}>
          {cta}
          <p className="small muted">{user ? 'Your latest month is waiting.' : 'Connect Gmail once and this month’s payments appear.'}</p>
        </div>
      </section>
    </>
  )
}

function MonthBand() {
  const [ref, shown] = useInView({ threshold: 0.3 })
  const total = demo.breakdown.reduce((s, r) => s + r.total, 0) + demo.unsure.total
  return (
    <section ref={ref} className={`${styles.band} ${styles.grow} ${shown ? styles.shownBars : ''}`} aria-labelledby="month-h">
      <div className={`page ${styles.bandInner}`}>
        <div className={styles.bandText}>
          <p className="caption">{formatMonth(demo.month)}, from the developer&apos;s own account</p>
          <h2 id="month-h" className="display">Where the money went.</h2>
          <p className={`display num ${styles.monthTotal}`}>{formatINR(total)}</p>
          <p className={styles.bandNote}>Nothing was typed. Every row came from a bank alert.</p>
        </div>
        <CategoryBreakdown rows={demo.breakdown} unsure={demo.unsure} />
      </div>
    </section>
  )
}

function Receipt() {
  const lines = [
    ['Payments tested', '312'],
    ['Never seen in training', 'all 312'],
    ['Right category', '87.8%'],
    ['95% range', '84.3 to 91.3%'],
    ['Macro-F1', '0.745'],
    ['Source', "one person's payments"],
  ]
  return (
    <figure className={styles.receipt}>
      <p className={styles.receiptHead}>SpendStream accuracy test</p>
      <dl>
        {lines.map(([k, v]) => <div key={k}><dt>{k}</dt><dd>{v}</dd></div>)}
      </dl>
      <p className={styles.receiptFoot}>Below 50% sure: asks you</p>
    </figure>
  )
}

function steps() {
  const top = demo ? [...demo.breakdown].sort((a, b) => b.total - a.total).slice(0, 3) : []
  return [
    {
      title: 'Your bank emails you',
      body: 'Every UPI debit already sends an alert with the amount, the date and who you paid. SpendStream searches Gmail for those alerts and nothing else. The first sync reads this month so far.',
      art: demo?.alert && <samp className={styles.alertArt}>{demo.alert.text}</samp>,
    },
    {
      title: 'It works out who you paid',
      body: 'UPI handles become names you recognise, and a payment to a person is told apart from a payment to a shop, even through a QR code.',
      art: demo?.cleaning?.length > 0 && (
        <ul className={styles.pairs}>
          {demo.cleaning.map(c => <li key={c.raw}><code>{c.raw}</code><strong>{c.clean}</strong></li>)}
        </ul>
      ),
    },
    {
      title: 'It files it, or asks',
      body: 'Your own rules come first, then a small model. When the model is less than 50% sure, the payment waits for you as Unsure.',
      art: demo && (
        <ul className={styles.filed}>
          {top.map(r => <li key={r.category}><CategoryChip category={r.category} /><span className="num">{formatINR(r.total)}</span></li>)}
          {demo.unsure.count > 0 && <li><CategoryChip category={null} /><span className="num pencil">{formatINR(demo.unsure.total)}</span></li>}
        </ul>
      ),
    },
    {
      title: 'You correct it once',
      body: 'Pick a category for one payment and every past and future payment to that exact merchant follows. A shop with a similar name stays as it is. Try it:',
      art: demo?.correction && <CorrectionDemo correction={demo.correction} />,
    },
  ]
}

// A real correction from the developer's account, replayed locally: the
// merchant, and how many of its other payments the rule moved, are real.
// Nothing here writes anywhere.
export function CorrectionDemo({ correction }) {
  const { row, also_updated: alsoUpdated } = correction
  const [category, setCategory] = useState(row.category)
  const [picking, setPicking] = useState(false)
  const [result, setResult] = useState(null)

  const pick = (cat) => {
    setPicking(false)
    setCategory(cat)
    setResult({
      text: alsoUpdated > 0
        ? `Saved. Also updated ${plural(alsoUpdated, 'other payment')} to ${row.merchant}.`
        : 'Saved.',
    })
  }
  const reset = () => { setCategory(row.category); setResult(null) }

  return (
    <div className={styles.correction}>
      <ResultNotice result={result} onClose={() => setResult(null)} />
      <table className="ledger">
        <tbody>
          <tr>
            <td className="num">{formatDay(row.date)}</td>
            <th scope="row">{row.merchant}</th>
            <td>
              <CategoryChip
                category={category}
                label={`Category for ${row.merchant}: ${category || 'Unsure'}. Change`}
                onClick={() => setPicking(true)}
              />
            </td>
            <td className="amount num">{formatINR(row.amount, { paise: true })}</td>
          </tr>
        </tbody>
      </table>
      <p className="small">
        Press the stamp to change it.{' '}
        {result && <button type="button" className="btn btn-text" onClick={reset}>Reset example</button>}
      </p>
      <p className="small muted">From the developer&apos;s own account, {formatMonth(row.date)}. Nothing here is saved.</p>
      {picking && (
        <CategoryPicker merchant={row.merchant} current={category} onPick={pick} onClose={() => setPicking(false)} />
      )}
    </div>
  )
}
