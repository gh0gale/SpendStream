import { useState, useEffect } from 'react'
import styles from './LandingPage.module.css'

const FEATURES = [
  {
    icon: '◈',
    title: 'Gmail Intelligence',
    desc: 'Automatically extracts transactions from bank alert emails. No manual entry — ever.',
    tag: 'Inbox → Data',
  },
  {
    icon: '◎',
    title: 'ML Categorisation',
    desc: 'Every transaction intelligently labelled — food, investments, transfers and more.',
    tag: 'Smart Tagging',
  },
  {
    icon: '◇',
    title: 'Medallion Pipeline',
    desc: 'Raw → Bronze → Silver → Gold. Your data is cleaned and enriched automatically.',
    tag: 'Auto Enrichment',
  },
  {
    icon: '◉',
    title: 'Real-time Insights',
    desc: 'Live dashboard with category breakdowns and monthly spending trends at a glance.',
    tag: 'Live Analytics',
  },
]

const STATS = [
  { value: '12+', label: 'Categories' },
  { value: '99%', label: 'Accuracy' },
  { value: '∞',   label: 'Transactions' },
]

const PIPELINE = [
  { layer: 'Raw',    desc: 'Gmail + CSV ingest' },
  { layer: 'Bronze', desc: 'Deduplicated + fingerprinted' },
  { layer: 'Silver', desc: 'ML categorised' },
  { layer: 'Gold',   desc: 'Aggregated insights' },
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
      { threshold: 0.12 }
    )
    document.querySelectorAll('[data-id]').forEach(el => observer.observe(el))
    return () => observer.disconnect()
  }, [])

  const vis = (id) => visible[id] ? styles.fadeIn : ''

  return (
    <div className={styles.root}>
      {/* Ambient layers */}
      <div className={styles.ambientA} />
      <div className={styles.ambientB} />
      <div className={styles.ambientC} />
      <div className={styles.gridTexture} />

      {/* ── Nav ── */}
      <nav className={`${styles.nav} ${scrolled ? styles.navScrolled : ''}`}>
        <div className={styles.navInner}>
          <button className={styles.logo} onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })}>
            <span className={styles.logoMark}>◈</span>
            SpendStream
          </button>
          <div className={styles.navActions}>
            <button className={styles.navLink} onClick={() => onNavigate('login')}>Sign in</button>
            <button className={styles.navCta}  onClick={() => onNavigate('login')}>
              Get started
              <span className={styles.navCtaArrow}>↗</span>
            </button>
          </div>
        </div>
      </nav>

      {/* ── Hero ── */}
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
            SpendStream connects to your Gmail, reads bank alerts, and automatically
            categorises every transaction using ML. Zero effort. Complete clarity.
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

      {/* ── Features ── */}
      <section className={styles.features}>
        <div className={styles.sectionInner}>
          <div data-id="feat-head" className={`${styles.sectionHead} ${vis('feat-head')}`}>
            <p className={styles.eyebrow}>Built different</p>
            <h2 className={styles.sectionTitle}>
              Finance intelligence,<br />not just a spreadsheet.
            </h2>
          </div>

          <div className={styles.featGrid}>
            {FEATURES.map((f, i) => (
              <div
                key={f.title}
                data-id={`feat-${i}`}
                className={`${styles.featCard} ${vis(`feat-${i}`)}`}
                style={{ transitionDelay: `${i * 70}ms` }}
              >
                <div className={styles.featCardInner}>
                  <div className={styles.featTop}>
                    <span className={styles.featIcon}>{f.icon}</span>
                    <span className={styles.featTag}>{f.tag}</span>
                  </div>
                  <h3 className={styles.featTitle}>{f.title}</h3>
                  <p className={styles.featDesc}>{f.desc}</p>
                </div>
                <div className={styles.featGlow} />
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Pipeline ── */}
      <section className={styles.pipeline}>
        <div className={styles.sectionInner}>
          <div data-id="pipe-head" className={`${styles.sectionHead} ${vis('pipe-head')}`}>
            <p className={styles.eyebrow}>How your data flows</p>
            <h2 className={styles.sectionTitle}>
              From inbox to insight.<br />Automatically.
            </h2>
          </div>

          <div data-id="pipe-flow" className={`${styles.pipeFlow} ${vis('pipe-flow')}`}>
            {PIPELINE.map(({ layer, desc }, i) => (
              <div key={layer} className={styles.pipeStep}>
                <div className={styles.pipeNode}>
                  <span className={styles.pipeIndex}>0{i + 1}</span>
                  <span className={styles.pipeLabel}>{layer}</span>
                  <span className={styles.pipeDesc}>{desc}</span>
                </div>
                {i < 3 && <div className={styles.pipeArrow}><span>→</span></div>}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── CTA ── */}
      <section className={styles.ctaSection}>
        <div data-id="cta-block" className={`${styles.ctaBlock} ${vis('cta-block')}`}>
          <div className={styles.ctaGlowRing} />
          <p className={styles.eyebrow}>Get started today</p>
          <h2 className={styles.ctaTitle}>
            Ready to see your<br />
            <em>money clearly?</em>
          </h2>
          <p className={styles.ctaSub}>Connect Gmail once. Everything else is automatic.</p>
          <button className={styles.ctaPrimary} onClick={() => onNavigate('login')}>
            Get started free
            <span className={styles.ctaArrow}>→</span>
          </button>
        </div>
      </section>

      {/* ── Footer ── */}
      <footer className={styles.footer}>
        <div className={styles.footerInner}>
          <span className={styles.logo}>
            <span className={styles.logoMark}>◈</span>
            SpendStream
          </span>
          <p className={styles.footerNote}>ML · FastAPI · Supabase · Built with care</p>
        </div>
      </footer>
    </div>
  )
}

/* ── Mini Dashboard Preview ── */
function MiniDashboardPreview() {
  const bars = [
    { label: 'Food',       pct: 65, amt: '₹2,235', color: '#C9A84C' },
    { label: 'Investment', pct: 90, amt: '₹12,670', color: '#4FD1C5' },
    { label: 'Transfer',   pct: 48, amt: '₹6,801',  color: '#A78BFA' },
    { label: 'Health',     pct: 20, amt: '₹65',     color: '#FB7185' },
  ]

  return (
    <div className={styles.dashCard}>
      <div className={styles.dashGlow} />

      {/* Header */}
      <div className={styles.dashHeader}>
        <div>
          <p className={styles.dashMonth}>March 2026</p>
          <p className={styles.dashTotal}>₹21,771</p>
          <p className={styles.dashSub}>Total spent</p>
        </div>
        <div className={styles.dashLive}>
          <span className={styles.dashLiveDot} />
          LIVE
        </div>
      </div>

      {/* Bars */}
      <div className={styles.dashBars}>
        {bars.map(b => (
          <div key={b.label} className={styles.dashBarRow}>
            <div className={styles.dashBarMeta}>
              <span className={styles.dashBarLabel}>{b.label}</span>
              <span className={styles.dashBarAmt}>{b.amt}</span>
            </div>
            <div className={styles.dashBarTrack}>
              <div
                className={styles.dashBarFill}
                style={{ width: `${b.pct}%`, background: `linear-gradient(90deg, ${b.color}30, ${b.color})` }}
              />
            </div>
          </div>
        ))}
      </div>

      {/* Footer row */}
      <div className={styles.dashFooter}>
        <span className={styles.dashMeta}>ML categorised · 26 txns</span>
        <span className={styles.dashViewAll}>view all →</span>
      </div>
    </div>
  )
}