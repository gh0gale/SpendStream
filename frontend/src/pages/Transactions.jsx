import { useState, useEffect, useMemo } from 'react'
import { supabase } from '../lib/supabase'
import Navbar from '../components/Navbar'
import { CATEGORY_COLORS } from '../components/Charts'
import styles from './Transactions.module.css'

const PAGE_SIZE = 20

export default function Transactions({ user, onNavigate, onSignOut }) {
  const [rows, setRows]           = useState([])
  const [loading, setLoading]     = useState(true)
  const [search, setSearch]       = useState('')
  const [catFilter, setCatFilter] = useState('All')
  const [sortKey, setSortKey]     = useState('transaction_date')
  const [sortDir, setSortDir]     = useState('desc')
  const [page, setPage]           = useState(1)
  const [selected, setSelected]   = useState(null)

  useEffect(() => { loadTransactions() }, [])

  const loadTransactions = async () => {
    setLoading(true)
    try {
      const { data } = await supabase.from('silver_transactions').select('*')
        .eq('user_id', user.id).order('transaction_date', { ascending: false })
      setRows(data || [])
    } catch (e) { console.error(e) }
    finally { setLoading(false) }
  }

  const categories = useMemo(() => {
    const cats = [...new Set(rows.map(r => r.category).filter(Boolean))]
    return ['All', ...cats.sort()]
  }, [rows])

  const filtered = useMemo(() => {
    let r = [...rows]
    if (catFilter !== 'All') r = r.filter(x => x.category === catFilter)
    if (search.trim()) {
      const q = search.toLowerCase()
      r = r.filter(x => x.merchant?.toLowerCase().includes(q) || x.category?.toLowerCase().includes(q) || String(x.amount).includes(q))
    }
    r.sort((a, b) => {
      let av = a[sortKey], bv = b[sortKey]
      if (sortKey === 'amount') { av = Number(av); bv = Number(bv) }
      if (av < bv) return sortDir === 'asc' ? -1 : 1
      if (av > bv) return sortDir === 'asc' ? 1 : -1
      return 0
    })
    return r
  }, [rows, catFilter, search, sortKey, sortDir])

  const paginated  = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)
  const totalPages = Math.ceil(filtered.length / PAGE_SIZE)

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
    setPage(1)
  }

  const formatDate = (d) => {
    if (!d) return '—'
    return new Date(d).toLocaleDateString('en-IN', { day:'2-digit', month:'short', year:'numeric' })
  }
  const formatINR = (n) => '₹' + Number(n).toLocaleString('en-IN', { maximumFractionDigits:2, minimumFractionDigits:2 })
  const totalFiltered = filtered.reduce((s, r) => s + Number(r.amount), 0)

  const cols = [
    { key:'merchant',         label:'Merchant' },
    { key:'category',         label:'Category' },
    { key:'transaction_date', label:'Date' },
    { key:'amount',           label:'Amount' },
    { key:null,               label:'' },
  ]

  return (
    <div className={styles.page}>
      <Navbar user={user} currentPage="transactions" onNavigate={onNavigate} onSignOut={onSignOut} />
      {selected && <Modal row={selected} onClose={() => setSelected(null)} formatINR={formatINR} formatDate={formatDate} />}

      <div className={styles.inner}>
        <div className={styles.pageHeader}>
          <p className={styles.pageSubtitle}>Silver table</p>
          <div className={styles.pageTitleRow}>
            <h1 className={styles.pageTitle}>All transactions</h1>
            <div className={styles.chips}>
              <div className={styles.chip}><span className={styles.chipLabel}>Total</span><span className={styles.chipValue}>{loading ? '—' : formatINR(totalFiltered)}</span></div>
              <div className={styles.chip}><span className={styles.chipLabel}>Shown</span><span className={styles.chipValue}>{loading ? '—' : filtered.length}</span></div>
            </div>
          </div>
        </div>

        <div className={styles.filters}>
          <div className={styles.searchWrap}>
            <span className={styles.searchIcon}>⌕</span>
            <input className={styles.searchInput} value={search}
              onChange={e => { setSearch(e.target.value); setPage(1) }}
              placeholder="Search merchant, category…" />
          </div>
          <div className={styles.catFilters}>
            {categories.map(cat => {
              const color = CATEGORY_COLORS[cat]
              const active = catFilter === cat
              return (
                <button key={cat} onClick={() => { setCatFilter(cat); setPage(1) }}
                  className={`${styles.catBtn} ${active ? styles.active : ''}`}
                  style={active ? {
                    background: color ? color+'18' : 'var(--surface-2)',
                    borderColor: color ? color+'44' : 'var(--border-mid)',
                    color: color || 'var(--gold)',
                  } : {}}
                >
                  {cat !== 'All' && <span className={styles.catDot} style={{ background: color || '#888' }} />}
                  {cat}
                </button>
              )
            })}
          </div>
        </div>

        <div className={styles.table}>
          <div className={styles.tableHead}>
            {cols.map(col => (
              <div key={col.label}
                onClick={() => col.key && handleSort(col.key)}
                className={`${styles.thCell} ${!col.key ? styles.noSort : ''} ${sortKey === col.key ? styles.sorted : ''}`}
              >
                {col.label}
                {col.key && sortKey === col.key && <span className={styles.sortIcon}>{sortDir === 'asc' ? '↑' : '↓'}</span>}
              </div>
            ))}
          </div>

          {loading ? (
            <div className={styles.skeletonRows}>
              {[...Array(8)].map((_,i) => (
                <div key={i} className={styles.skeletonRow}
                  style={{ height:50, background:'var(--bg-3)', opacity: 0.7 - i*0.08 }} />
              ))}
            </div>
          ) : paginated.length === 0 ? (
            <div className={styles.emptyTable}>
              <span className={styles.emptyIcon}>◈</span>
              <p className={styles.emptyText}>{search || catFilter !== 'All' ? 'No transactions match your filters' : 'No transactions yet'}</p>
            </div>
          ) : (
            paginated.map((row, i) => {
              const color = CATEGORY_COLORS[row.category] || '#546e7a'
              return (
                <div key={row.id} className={styles.tableRow} onClick={() => setSelected(row)}>
                  <div className={styles.merchantCol}>
                    <span className={styles.merchantName}>{row.merchant || '—'}</span>
                    <span className={styles.merchantSrc}>{row.source}</span>
                  </div>
                  <div>
                    {row.category ? (
                      <span className={styles.badge} style={{ background: color+'18', border:`1px solid ${color}33`, color }}>
                        <span className={styles.badgeDot} style={{ background: color }} />
                        {row.category}
                      </span>
                    ) : <span className={styles.noBadge}>uncategorised</span>}
                  </div>
                  <span className={styles.dateCell}>{formatDate(row.transaction_date)}</span>
                  <span className={styles.amountCell}>{formatINR(row.amount)}</span>
                  <span className={styles.arrowCell}>→</span>
                </div>
              )
            })
          )}
        </div>

        {totalPages > 1 && (
          <div className={styles.pagination}>
            <p className={styles.paginationInfo}>Page {page} of {totalPages} · {filtered.length} results</p>
            <div className={styles.paginationBtns}>
              <button className={styles.pagBtn} onClick={() => setPage(p => Math.max(1, p-1))} disabled={page===1}>← Prev</button>
              <button className={styles.pagBtn} onClick={() => setPage(p => Math.min(totalPages, p+1))} disabled={page===totalPages}>Next →</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function Modal({ row, onClose, formatINR, formatDate }) {
  const color = CATEGORY_COLORS[row.category] || '#888'
  const fields = [
    { key:'Category',   val: row.category || 'Uncategorised', color: row.category ? color : undefined },
    { key:'Date',       val: formatDate(row.transaction_date) },
    { key:'Source',     val: row.source },
    { key:'Type',       val: row.transaction_type },
  ]
  return (
    <div className={styles.modalOverlay} onClick={onClose}>
      <div className={styles.modalCard} onClick={e => e.stopPropagation()}>
        <div className={styles.modalTopBar} style={{ background:`linear-gradient(90deg,transparent,${color},transparent)` }} />
        <button className={styles.modalClose} onClick={onClose}>×</button>

        <div className={styles.modalMeta}>
          <p className={styles.modalMetaLabel}>Transaction detail</p>
          <p className={styles.modalAmount}>{formatINR(row.amount)}</p>
          <p className={styles.modalMerchant}>{row.merchant}</p>
        </div>

        <div className={styles.modalFields}>
          {fields.map(f => (
            <div key={f.key} className={styles.modalField}>
              <span className={styles.modalFieldKey}>{f.key}</span>
              <span className={styles.modalFieldVal} style={{ color: f.color || 'var(--text-primary)' }}>{f.val || '—'}</span>
            </div>
          ))}
        </div>

        {row.raw_text && (
          <div className={styles.modalRaw}>
            <p className={styles.modalRawLabel}>Raw</p>
            <p className={styles.modalRawText}>{row.raw_text?.slice(0,200)}</p>
          </div>
        )}
      </div>
    </div>
  )
}