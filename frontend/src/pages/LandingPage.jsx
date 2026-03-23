import { useState, useEffect } from 'react'
import styles from './LandingPage.module.css'

const FEATURES = [
  { icon: '◈', title: 'Gmail Intelligence',   desc: 'Automatically extracts transactions from bank alert emails. No manual entry ever.' },
  { icon: '◎', title: 'ML Categorisation',    desc: 'Every transaction is intelligently categorised — food, investments, transfers and more.' },
  { icon: '◇', title: 'Medallion Pipeline',   desc: 'Raw → Bronze → Silver → Gold. Your data is cleaned and enriched automatically.' },
  { icon: '◉', title: 'Real-time Insights',   desc: 'Live dashboard with category breakdowns and monthly spending trends.' },
]
const STATS = [
  { value: '12+', label: 'Categories' },
  { value: '99%', label: 'Accuracy' },
  { value: '∞',   label: 'Transactions' },
]

export default function LandingPage({ onNavigate }) {
  const [scrolled, setScrolled] = useState(false)
  const [visible, setVisible]   = useState({})

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 40)
    window.addEventListener('scroll', onScroll)
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => entries.forEach(e => {
        if (e.isIntersecting) setVisible(v => ({ ...v, [e.target.dataset.id]: true }))
      }),
      { threshold: 0.15 }
    )
    document.querySelectorAll('[data-id]').forEach(el => observer.observe(el))
    return () => observer.disconnect()
  }, [])

  return (
    <div className={styles.root}>
      <div className={styles.noise} />
      <div className={styles.orb1} />
      <div className={styles.orb2} />

      <nav className={`${styles.nav} ${scrolled ? styles.navScrolled : ''}`}>
        <div className={styles.navInner}>
          <span className={styles.logo}>
            <span className={styles.logoMark}>◈</span>
            SpendStream
          </span>
          <div className={styles.navActions}>
            <button className={styles.navLink} onClick={() => onNavigate('login')}>Sign in</button>
            <button className={styles.navCta}  onClick={() => onNavigate('login')}>Get started</button>
          </div>
        </div>
      </nav>

      <section className={styles.hero}>
        <div className={styles.heroContent}>
          <div className={styles.heroBadge}>
            <span className={styles.badgeDot} />
            AI-powered expense intelligence
          </div>
          <h1 className={styles.heroTitle}>
            Know exactly where<br />
            <em>every rupee</em> goes.
          </h1>
          <p className={styles.heroSub}>
            SpendStream connects to your Gmail, reads bank alerts,
            and automatically categorises every transaction using ML.
            Zero effort. Complete clarity.
          </p>
          <div className={styles.heroCtas}>
            <button className={styles.ctaPrimary} onClick={() => onNavigate('login')}>
              Start tracking free
              <span className={styles.ctaArrow}>→</span>
            </button>
            <button className={styles.ctaGhost}>See how it works</button>
          </div>
          <div className={styles.statsStrip}>
            {STATS.map(s => (
              <div key={s.label} className={styles.stat}>
                <span className={styles.statValue}>{s.value}</span>
                <span className={styles.statLabel}>{s.label}</span>
              </div>
            ))}
          </div>
        </div>
        <div className={styles.heroVisual}>
          <MiniDashboardPreview />
        </div>
      </section>

      <section className={styles.features}>
        <div className={styles.sectionInner}>
          <div data-id="feat-head" className={`${styles.sectionHead} ${visible['feat-head'] ? styles.fadeIn : ''}`}>
            <p className={styles.eyebrow}>Built different</p>
            <h2 className={styles.sectionTitle}>Finance intelligence,<br />not just a spreadsheet.</h2>
          </div>
          <div className={styles.featGrid}>
            {FEATURES.map((f, i) => (
              <div key={f.title} data-id={`feat-${i}`}
                className={`${styles.featCard} ${visible[`feat-${i}`] ? styles.fadeIn : ''}`}
                style={{ transitionDelay: `${i * 60}ms` }}
              >
                <span className={styles.featIcon}>{f.icon}</span>
                <h3 className={styles.featTitle}>{f.title}</h3>
                <p className={styles.featDesc}>{f.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className={styles.pipeline}>
        <div className={styles.sectionInner}>
          <div data-id="pipe-head" className={`${styles.sectionHead} ${visible['pipe-head'] ? styles.fadeIn : ''}`}>
            <p className={styles.eyebrow}>Data architecture</p>
            <h2 className={styles.sectionTitle}>Medallion pipeline.<br />Enterprise-grade, personal scale.</h2>
          </div>
          <div data-id="pipe-flow" className={`${styles.pipeFlow} ${visible['pipe-flow'] ? styles.fadeIn : ''}`}>
            {['Raw', 'Bronze', 'Silver', 'Gold'].map((layer, i) => (
              <div key={layer} className={styles.pipeStep}>
                <div className={styles.pipeNode}>
                  <span className={styles.pipeLabel}>{layer}</span>
                  <span className={styles.pipeDesc}>{['Gmail + CSV ingest','Deduplicated + fingerprinted','ML categorised','Aggregated insights'][i]}</span>
                </div>
                {i < 3 && <div className={styles.pipeArrow}>→</div>}
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className={styles.ctaSection}>
        <div data-id="cta-block" className={`${styles.ctaBlock} ${visible['cta-block'] ? styles.fadeIn : ''}`}>
          <h2 className={styles.ctaTitle}>Ready to see<br />your money clearly?</h2>
          <p className={styles.ctaSub}>Connect Gmail once. Everything else is automatic.</p>
          <button className={styles.ctaPrimary} onClick={() => onNavigate('login')}>
            Get started free
            <span className={styles.ctaArrow}>→</span>
          </button>
        </div>
      </section>

      <footer className={styles.footer}>
        <span className={styles.logo}>
          <span className={styles.logoMark}>◈</span>
          SpendStream
        </span>
        <p className={styles.footerNote}>Built with ML + FastAPI + Supabase</p>
      </footer>
    </div>
  )
}

function MiniDashboardPreview() {
  const bars = [
    { label: 'Food',       pct: 65, amt: '₹2,235' },
    { label: 'Investment', pct: 90, amt: '₹12,670' },
    { label: 'Transfer',   pct: 48, amt: '₹6,801' },
    { label: 'Health',     pct: 20, amt: '₹65' },
  ]
  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 'var(--r-xl)', padding: '28px 24px',
      width: '100%', maxWidth: 340,
      boxShadow: '0 40px 80px rgba(0,0,0,0.6), 0 0 60px rgba(212,168,83,0.04)',
    }}>
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:22 }}>
        <div>
          <p style={{ fontSize:11, color:'var(--text-tertiary)', fontFamily:'var(--font-mono)', letterSpacing:'0.06em', textTransform:'uppercase' }}>March 2026</p>
          <p style={{ fontSize:26, fontWeight:600, letterSpacing:'-0.03em', marginTop:4, color:'var(--text-primary)' }}>₹21,771</p>
          <p style={{ fontSize:12, color:'var(--text-secondary)', marginTop:2 }}>Total spent</p>
        </div>
        <div style={{
          background:'var(--gold-dim)', border:'1px solid var(--gold-border)',
          borderRadius:'var(--r-xs)', padding:'4px 9px',
          fontSize:10, color:'var(--gold)', fontFamily:'var(--font-mono)', letterSpacing:'0.06em',
        }}>LIVE</div>
      </div>
      <div style={{ display:'flex', flexDirection:'column', gap:14 }}>
        {bars.map(b => (
          <div key={b.label}>
            <div style={{ display:'flex', justifyContent:'space-between', marginBottom:6 }}>
              <span style={{ fontSize:12, color:'var(--text-secondary)', fontWeight:400 }}>{b.label}</span>
              <span style={{ fontSize:12, fontFamily:'var(--font-mono)', color:'var(--text-primary)' }}>{b.amt}</span>
            </div>
            <div style={{ height:3, background:'var(--bg-3)', borderRadius:2, overflow:'hidden' }}>
              <div style={{ height:'100%', width:`${b.pct}%`, background:'linear-gradient(90deg, var(--gold-dim), var(--gold))', borderRadius:2 }} />
            </div>
          </div>
        ))}
      </div>
      <div style={{ marginTop:20, paddingTop:16, borderTop:'1px solid var(--border)', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
        <span style={{ fontSize:11, color:'var(--text-tertiary)', fontFamily:'var(--font-mono)' }}>ML categorised · 26 txns</span>
        <span style={{ fontSize:11, color:'var(--gold)', fontFamily:'var(--font-mono)' }}>view all →</span>
      </div>
    </div>
  )
}