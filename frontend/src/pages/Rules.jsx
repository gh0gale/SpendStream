import { useCallback, useState } from 'react'
import { Link } from 'react-router'
import CategoryChip from '../components/CategoryChip'
import CategoryPicker from '../components/CategoryPicker'
import ResultNotice from '../components/ResultNotice'
import { ErrorNotice, SkeletonRows } from '../components/States'
import { useAuth } from '../lib/auth'
import { plural } from '../lib/format'
import { supabase } from '../lib/supabase'
import { useCorrection } from '../lib/useCorrection'
import { must, useLoad } from '../lib/useLoad'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Rules.module.css'

// ponytail: the most recent rules only; page it if anyone has more than this.
const RULE_CAP = 200
const NAME_CHUNK = 50          // merchant keys per name lookup, so the request URL stays short

// The rule stores only the merchant key. Its display name and one payment to
// change it through come from the user's own payments.
const keyLabel = (key) => key.replace(/^(upi|name):/, '')

export default function Rules() {
  usePageTitle('Your rules')
  const { user } = useAuth()
  const [picking, setPicking] = useState(null)
  const [removing, setRemoving] = useState(null)       // merchant key being removed
  const [result, setResult] = useState(null)
  const { saving, correct } = useCorrection()

  const fetcher = useCallback(async () => {
    const rules = await must(supabase.from('user_merchant_rules')
      .select('merchant_key, category, is_person, updated_at')
      .eq('user_id', user.id).order('updated_at', { ascending: false }).range(0, RULE_CAP - 1))
    const byKey = new Map()
    for (let i = 0; i < rules.length; i += NAME_CHUNK) {
      const keys = rules.slice(i, i + NAME_CHUNK).map(r => r.merchant_key)
      const rows = await must(supabase.from('silver_transactions')
        .select('id, merchant, merchant_key, category')
        .eq('user_id', user.id).in('merchant_key', keys).range(0, 999))
      for (const r of rows) if (!byKey.has(r.merchant_key)) byKey.set(r.merchant_key, r)
    }
    return rules.map(rule => ({ ...rule, row: byKey.get(rule.merchant_key) ?? null }))
  }, [user.id])
  const list = useLoad(fetcher)
  const rules = list.data ?? []

  const nameOf = (rule) => rule.row?.merchant || keyLabel(rule.merchant_key)

  const handlePick = async (cat) => {
    const rule = picking
    if (cat === rule.category) { setPicking(null); return }
    try {
      const { alsoUpdated } = await correct({ ...rule.row, category: rule.category }, cat, 'rules')
      setPicking(null)
      setResult({ text: alsoUpdated > 0
        ? `Saved. ${nameOf(rule)} is now ${cat}. Also updated ${plural(alsoUpdated, 'other payment')}.`
        : `Saved. ${nameOf(rule)} is now ${cat}.` })
      list.reload()
    } catch (err) {
      setPicking(null)
      setResult({ error: true, text: `Couldn't save. ${err.message}`, retry: () => setPicking(rule) })
    }
  }

  const handleRemove = async (rule) => {
    setRemoving(rule.merchant_key)
    try {
      const { error } = await supabase.rpc('delete_merchant_rule', { p_merchant_key: rule.merchant_key })
      if (error) throw new Error(error.message)
      setResult({ text: `Rule for ${nameOf(rule)} removed. Payments already sorted keep their category; new ones are guessed again.` })
      list.reload()
    } catch (err) {
      setResult({ error: true, text: `Couldn't remove it. ${err.message}`, retry: () => handleRemove(rule) })
    } finally {
      setRemoving(null)
    }
  }

  return (
    <div className={styles.page}>
      <header className="page-head">
        <p className="caption">What SpendStream remembers</p>
        <h1 className="display">Your <span className="mark">rules</span></h1>
      </header>
      <p className="muted">
        Every time you pick a category for a payment, SpendStream remembers it for that merchant and
        uses it for new payments too. Change or remove one here.
      </p>

      <ResultNotice result={result} onClose={() => setResult(null)} onRetry={() => { const r = result.retry; setResult(null); r() }} />

      {list.status === 'error' ? (
        <ErrorNotice title="Couldn't load your rules." detail={list.detail} onRetry={list.reload} />
      ) : !list.data ? (
        <SkeletonRows rows={5} height={48} />
      ) : rules.length === 0 ? (
        <div className={`card ${styles.empty}`}>
          <h2>No rules yet</h2>
          <p className="muted">Pick a category on any payment and it will show up here.</p>
          <Link to="/app/transactions" className="link-arrow">Go to your payments</Link>
        </div>
      ) : (
        <>
          <ul className={styles.list}>
            {rules.map(rule => (
              <li key={rule.merchant_key} className={styles.item}>
                <span className={`monogram ${rule.is_person ? 'monogram-person' : ''}`} aria-hidden="true">
                  {nameOf(rule).trim().charAt(0)}
                </span>
                <div className={styles.name}>
                  <p className={styles.merchant}>
                    {nameOf(rule)}
                    {rule.is_person && <span className="small muted"> to a person</span>}
                  </p>
                </div>
                <CategoryChip
                  category={rule.category}
                  label={`Category rule for ${nameOf(rule)}: ${rule.category}.${rule.row ? ' Change' : ''}`}
                  onClick={rule.row ? () => setPicking(rule) : undefined}
                />
                <button
                  type="button"
                  className="btn btn-text"
                  onClick={() => handleRemove(rule)}
                  disabled={removing === rule.merchant_key}
                  aria-label={`Remove the rule for ${nameOf(rule)}`}
                >
                  {removing === rule.merchant_key ? 'Removing' : 'Remove'}
                </button>
              </li>
            ))}
          </ul>
          {rules.length === RULE_CAP && <p className="small muted">Showing the {RULE_CAP} most recent rules.</p>}
        </>
      )}

      {picking && (
        <CategoryPicker
          merchant={nameOf(picking)}
          current={picking.category}
          saving={saving}
          onPick={handlePick}
          onClose={() => setPicking(null)}
        />
      )}
    </div>
  )
}
