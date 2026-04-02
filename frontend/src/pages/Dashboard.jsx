import { useState, useEffect } from 'react'
import { supabase } from '../lib/supabase'
import Navbar from '../components/Navbar'
import StatCard from '../components/StatCard'
import { DonutChart, BarChart, CATEGORY_COLORS } from '../components/Charts'
import styles from './Dashboard.module.css'

const API = 'https://spendstream-api.onrender.com'

export default function Dashboard({ user, onNavigate, onSignOut }) {
  const [goldData, setGoldData]           = useState([])
  const [loading, setLoading]             = useState(true)
  const [uploading, setUploading]         = useState(false)
  const [fetching, setFetching]           = useState(false)
  const [toast, setToast]                 = useState(null)
  const [selectedCategory, setSelectedCategory] = useState(null)
  const [chartView, setChartView]         = useState('donut')
  
  // NEW: Track if Gmail is connected
  const [isGmailConnected, setIsGmailConnected] = useState(false)

  useEffect(() => { 
    loadGoldData()
    checkGmailConnection() // NEW: Check connection on load
  }, [])

  // NEW: Query Supabase to see if the user has tokens
  const checkGmailConnection = async () => {
    console.log("Checking Gmail connection for user:", user.id);
    try {
      const { data, error } = await supabase
        .from('gmail_sync')
        .select('user_id') // <--- CHANGED FROM 'id' TO 'user_id'
        .eq('user_id', user.id)
        .limit(1);
        
      console.log("Supabase Response - Data:", data, "Error:", error);

      if (error) {
        console.error("Supabase returned an error:", error.message);
        return;
      }

      if (data && data.length > 0) {
        console.log("Tokens found! Setting isGmailConnected to true.");
        setIsGmailConnected(true);
      } else {
        console.log("No tokens found. Supabase returned an empty array.");
      }
    } catch (err) {
      console.error("Network/try-catch error:", err);
    }
  }

  const loadGoldData = async () => {
    setLoading(true)
    try {
      const { data } = await supabase.from('gold_monthly_summary').select('*')
        .eq('user_id', user.id).order('month', { ascending: false })
      setGoldData(data || [])
    } catch { showToast('Failed to load data', 'error') }
    finally { setLoading(false) }
  }

  const getToken = async () => {
    const { data } = await supabase.auth.getSession()
    return data.session.access_token
  }

  const showToast = (msg, type = 'success') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3500)
  }



  // NEW: The Polling Function
  const pollTaskStatus = async (taskId, taskType) => {
    try {
      const token = await getToken()
      const res = await fetch(`${API}/task/${taskId}`, {
        headers: { Authorization: `Bearer ${token}` }
      })
      const data = await res.json()

      if (data.status === 'SUCCESS') {
        showToast('Processing complete! Dashboard updated.')
        await loadGoldData()
        // Turn off the respective loading spinners
        if (taskType === 'upload') setUploading(false)
        if (taskType === 'gmail') setFetching(false)
      } else if (data.status === 'FAILURE') {
        showToast('Background processing failed.', 'error')
        if (taskType === 'upload') setUploading(false)
        if (taskType === 'gmail') setFetching(false)
      } else {
        // Status is PENDING, STARTED, or RETRY. Wait 3 seconds and check again.
        setTimeout(() => pollTaskStatus(taskId, taskType), 3000)
      }
    } catch (err) {
      console.error("Polling error:", err)
      showToast('Error checking status.', 'error')
      if (taskType === 'upload') setUploading(false)
      if (taskType === 'gmail') setFetching(false)
    }
  }

  const handleFileUpload = async (e) => {
    const file = e.target.files[0]
    if (!file) return
    const allowed = ['.csv', '.xlsx', '.xls']
    if (!allowed.some(ext => file.name.toLowerCase().endsWith(ext))) {
      showToast('Please upload a CSV or Excel file', 'error'); return
    }
    
    setUploading(true) // Start the spinner
    try {
      const token = await getToken()
      const formData = new FormData()
      formData.append('file', file)
      const res = await fetch(`${API}/upload-file`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}` }, body: formData
      })
      const result = await res.json()
      if (!res.ok) throw new Error(result.detail || 'Upload failed')
      
      showToast('Upload accepted. Analyzing transactions...')
      
      // Instead of blindly waiting 3 seconds, start polling the task!
      if (result.task_id) {
        pollTaskStatus(result.task_id, 'upload')
      } else {
        // Fallback just in case backend didn't return a task ID
        setUploading(false)
        loadGoldData()
      }
    } catch (err) { 
      showToast(err.message, 'error') 
      setUploading(false)
    } finally { 
      e.target.value = '' 
      
    }
  }

  const handleGmailConnect = async () => {
    const { data } = await supabase.auth.getSession()
    window.location.href = `${API}/auth/google?token=${data.session.access_token}`
  }

  const handleGmailFetch = async () => {
    setFetching(true) // Start the spinner
    try {
      const token = await getToken()
      const res = await fetch(`${API}/fetch-gmail`, { headers: { Authorization: `Bearer ${token}` } })
      const result = await res.json()
      if (!res.ok) throw new Error(result.detail || result.error || 'Fetch failed')
      
      showToast('Sync started. Categorizing emails in background...')
      
      // Start polling the Celery task
      if (result.task_id) {
        pollTaskStatus(result.task_id, 'gmail')
      } else {
        setFetching(false)
      }
    } catch (err) { 
      showToast(err.message, 'error') 
      setFetching(false)
    }
    // Again, we do NOT setFetching(false) here. The poller handles it.
  }

// ... [The rest of your component (latestMonth, charts, JSX) remains exactly the same!] ...

  const latestMonth = goldData[0]?.month
  const monthData   = goldData.filter(d => d.month === latestMonth)
  const totalSpend  = monthData.reduce((s, d) => s + Number(d.total_amount), 0)
  const totalTxns   = monthData.reduce((s, d) => s + d.txn_count, 0)
  const topCategory = [...monthData].sort((a,b) => b.total_amount - a.total_amount)[0]

  const formatMonth = (m) => {
    if (!m) return '—'
    return new Date(m).toLocaleDateString('en-IN', { month: 'long', year: 'numeric' })
  }
  const formatINR = (n) => '₹' + Number(n).toLocaleString('en-IN', { maximumFractionDigits: 0 })

  return (
    <div className={styles.page}>
      <Navbar user={user} currentPage="dashboard" onNavigate={onNavigate} onSignOut={onSignOut} />

      {toast && (
        <div className={`${styles.toast} ${styles[toast.type]}`}>{toast.msg}</div>
      )}

      <div className={styles.inner}>
        {/* Header */}
        <div className={styles.pageHeader}>
          <div>
            <p className={styles.pageSubtitle}>{formatMonth(latestMonth)}</p>
            <h1 className={styles.pageTitle}>Spending overview</h1>
          </div>
          <div className={styles.actions}>
            <label className={`${styles.uploadLabel} ${uploading ? styles.disabled : ''}`}>
              <input type="file" accept=".csv,.xlsx,.xls" onChange={handleFileUpload} style={{ display:'none' }} disabled={uploading} />
              {uploading ? <MiniSpinner /> : '↑'}
              {uploading ? 'Uploading…' : 'Upload CSV'}
            </label>

            {/* NEW: Conditional Gmail Connect Button */}
            {isGmailConnected ? (
              <ActionBtn disabled variant="secondary" icon="✓">
                Connected
              </ActionBtn>
            ) : (
              <ActionBtn onClick={handleGmailConnect} icon="✉" variant="secondary">
                Connect Gmail
              </ActionBtn>
            )}

            <ActionBtn onClick={handleGmailFetch} icon="↓" loading={fetching} variant="primary">
              {fetching ? 'Syncing…' : 'Sync Gmail'}
            </ActionBtn>
          </div>
        </div>

        {/* Stats */}
        <div className={styles.statGrid}>
          <StatCard label="Total spend"   value={loading ? '—' : formatINR(totalSpend)}  sub={`${totalTxns} transactions`} accent icon="◈" loading={loading} />
          <StatCard label="Top category"  value={loading ? '—' : (topCategory?.category || '—')} sub={topCategory ? formatINR(topCategory.total_amount) : ''} icon="◎" loading={loading} />
          <StatCard label="Categories"    value={loading ? '—' : monthData.length} sub="tracked this month" icon="◇" loading={loading} />
          <StatCard label="Avg per txn"   value={loading || !totalTxns ? '—' : formatINR(totalSpend / totalTxns)} sub="this month" icon="◉" loading={loading} />
        </div>

        {/* Charts */}
        <div className={styles.chartsRow}>
          {/* Donut / Bar */}
          <div className={styles.chartCard}>
            <div className={styles.chartCardHeader}>
              <p className={styles.chartCardTitle}>By category</p>
              <div className={styles.viewToggle}>
                {['donut','bar'].map(v => (
                  <button key={v} onClick={() => setChartView(v)}
                    className={`${styles.viewBtn} ${chartView === v ? styles.active : ''}`}>{v}</button>
                ))}
              </div>
            </div>
            <div className={styles.chartArea}>
              {loading ? <div className={styles.chartSkeleton} />
                : monthData.length === 0 ? <EmptyState />
                : chartView === 'donut'
                  ? <DonutChart data={monthData} onSliceClick={setSelectedCategory} />
                  : <BarChart data={monthData} />
              }
            </div>
            {selectedCategory && chartView === 'donut' && (
              <div className={styles.catDetail} style={{ borderColor: (CATEGORY_COLORS[selectedCategory.category] || '#888') + '33' }}>
                <div className={styles.catDetailInner}>
                  <div>
                    <p className={styles.catName} style={{ color: CATEGORY_COLORS[selectedCategory.category] || 'var(--text-primary)' }}>
                      {selectedCategory.category}
                    </p>
                    <p className={styles.catTxns}>{selectedCategory.txn_count} transactions</p>
                  </div>
                  <p className={styles.catAmount}>{formatINR(selectedCategory.total_amount)}</p>
                  <button className={styles.catClose} onClick={() => setSelectedCategory(null)}>×</button>
                </div>
              </div>
            )}
          </div>

          {/* Breakdown list */}
          <div className={styles.breakdownCard}>
            <p className={styles.breakdownTitle}>Breakdown</p>
            {loading ? (
              <div className={styles.skeletonList}>
                {[...Array(5)].map((_,i) => <div key={i} className={styles.skeletonItem} style={{ opacity: 0.6 - i*0.1 }} />)}
              </div>
            ) : monthData.length === 0 ? <EmptyState /> : (
              <div className={styles.breakdownList}>
                {[...monthData].sort((a,b) => b.total_amount - a.total_amount).map(item => {
                  const pct = totalSpend > 0 ? (item.total_amount / totalSpend * 100) : 0
                  const color = CATEGORY_COLORS[item.category] || '#546e7a'
                  return (
                    <div key={item.category}
                      onClick={() => setSelectedCategory(item)}
                      className={`${styles.breakdownRow} ${selectedCategory?.category === item.category ? styles.selected : ''}`}
                      style={{ borderColor: selectedCategory?.category === item.category ? color + '44' : 'transparent' }}
                    >
                      <div className={styles.breakdownMeta}>
                        <div className={styles.breakdownLeft}>
                          <div className={styles.breakdownDot} style={{ background: color }} />
                          <span className={styles.breakdownName}>{item.category}</span>
                          <span className={styles.breakdownTxns}>{item.txn_count} txns</span>
                        </div>
                        <span className={styles.breakdownAmt}>{formatINR(item.total_amount)}</span>
                      </div>
                      <div className={styles.progressBar}>
                        <div className={styles.progressFill} style={{ width:`${pct}%`, background: color }} />
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>

        {/* Banner */}
        <div className={styles.banner}>
          <div>
            <p className={styles.bannerTitle}>View all transactions</p>
            <p className={styles.bannerSub}>Detailed silver table with merchant, category, date and amount</p>
          </div>
          <button className={styles.bannerBtn} onClick={() => onNavigate('transactions')}>
            View transactions →
          </button>
        </div>
      </div>
    </div>
  )
}

function ActionBtn({ children, onClick, icon, loading, variant }) {
  return (
    <button onClick={onClick} disabled={loading}
      className={`${styles.actionBtn} ${styles[variant]}`}>
      {loading ? <span className={styles.miniSpinner} /> : icon}
      {children}
    </button>
  )
}
function MiniSpinner() {
  return <span className={styles.miniSpinner} />
}
function EmptyState() {
  return (
    <div className={styles.empty}>
      <span className={styles.emptyIcon}>◈</span>
      <p className={styles.emptyText}>No data yet — sync Gmail or upload a CSV</p>
    </div>
  )
}