import { supabase } from '../lib/supabase'
import { useState } from 'react'

const CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@300;400;500&display=swap');

  :root {
    --bg:      #07070f;
    --surface: rgba(14,14,26,0.80);
    --border:  rgba(255,255,255,0.07);
    --text:    #e8e8f0;
    --muted:   rgba(232,232,240,0.36);
    --pink:    #ff2d6b;
    --cyan:    #00e5c3;
    --violet:  #a855f7;
    --glow-p:  rgba(255,45,107,0.20);
    --glow-c:  rgba(0,229,195,0.13);
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  .lp {
    min-height: 100vh;
    background: var(--bg);
    font-family: 'Syne', sans-serif;
    color: var(--text);
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 40px 24px 64px;
    position: relative;
    overflow: hidden;
  }

  .lp::before {
    content: '';
    position: fixed; inset: 0;
    background-image:
      linear-gradient(rgba(255,255,255,0.025) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255,255,255,0.025) 1px, transparent 1px);
    background-size: 52px 52px;
    pointer-events: none; z-index: 0;
  }

  .orb {
    position: fixed; border-radius: 50%;
    filter: blur(90px); pointer-events: none; z-index: 0;
  }
  .orb-1 {
    width: 560px; height: 560px;
    top: -160px; left: -100px;
    background: var(--glow-p);
    animation: drift 14s ease-in-out infinite alternate;
  }
  .orb-2 {
    width: 420px; height: 420px;
    bottom: -100px; right: -80px;
    background: var(--glow-c);
    animation: drift 17s ease-in-out infinite alternate-reverse;
  }
  .orb-3 {
    width: 280px; height: 280px;
    top: 45%; left: 60%;
    background: rgba(168,85,247,0.10);
    animation: drift 20s ease-in-out infinite alternate;
  }
  @keyframes drift {
    from { transform: translate(0,0) scale(1); }
    to   { transform: translate(28px,20px) scale(1.09); }
  }

  .card {
    position: relative; z-index: 1;
    width: 100%; max-width: 500px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 22px;
    padding: 48px 44px 44px;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
    box-shadow:
      inset 0 0 0 1px rgba(255,255,255,0.04),
      0 0 0 1px rgba(0,0,0,0.5),
      0 32px 80px rgba(0,0,0,0.65),
      0 0 80px var(--glow-p);
    animation: cardIn 0.75s cubic-bezier(0.22,1,0.36,1) both;
  }
  @keyframes cardIn {
    from { opacity:0; transform: translateY(36px) scale(0.96); }
    to   { opacity:1; transform: translateY(0)    scale(1); }
  }

  .card::before {
    content: '';
    position: absolute;
    top: 0; left: 10%; right: 10%; height: 1px;
    background: linear-gradient(90deg, transparent, var(--pink), var(--cyan), transparent);
  }

  .pill {
    display: inline-flex; align-items: center; gap: 7px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 9.5px; font-weight: 500;
    letter-spacing: 0.13em; text-transform: uppercase;
    color: var(--pink);
    background: rgba(255,45,107,0.09);
    border: 1px solid rgba(255,45,107,0.22);
    padding: 5px 13px; border-radius: 100px;
    margin-bottom: 18px;
    animation: up 0.5s 0.12s both;
  }
  .pill-dot {
    width: 5px; height: 5px;
    background: var(--pink); border-radius: 50%;
    box-shadow: 0 0 7px var(--pink);
    animation: blink 2.2s ease-in-out infinite;
  }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }

  .title {
    font-size: clamp(24px,4.5vw,34px);
    font-weight: 800; letter-spacing: -0.8px;
    line-height: 1.12; color: #fff;
    margin-bottom: 7px;
    animation: up 0.5s 0.18s both;
  }
  .grad {
    background: linear-gradient(100deg, var(--pink) 0%, var(--cyan) 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
  }

  .email {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11.5px; font-weight: 300;
    color: var(--muted); letter-spacing: 0.04em;
    margin-bottom: 34px;
    animation: up 0.5s 0.22s both;
  }

  .rule {
    border: none; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.07), transparent);
    margin: 0 -44px 32px;
    animation: up 0.4s 0.26s both;
  }

  .label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 9px; font-weight: 500;
    letter-spacing: 0.17em; text-transform: uppercase;
    color: var(--muted); margin-bottom: 9px;
  }

  .sec { animation: up 0.5s both; }
  .sec:nth-of-type(1) { animation-delay: 0.29s; }
  .sec:nth-of-type(2) { animation-delay: 0.36s; }
  .sec:nth-of-type(3) { animation-delay: 0.43s; }
  .sec + .sec { margin-top: 20px; }

  .upload {
    position: relative; cursor: pointer;
    border: 1.5px dashed rgba(0,229,195,0.22);
    border-radius: 13px; padding: 28px 20px;
    text-align: center;
    background: rgba(0,229,195,0.025);
    transition: border-color .2s, background .2s, box-shadow .2s;
    overflow: hidden;
  }
  .upload:hover,
  .upload.drag {
    border-color: rgba(0,229,195,0.55);
    background: rgba(0,229,195,0.055);
    box-shadow: 0 0 28px rgba(0,229,195,0.09);
  }
  .upload:hover .upload-icon { transform: translateY(-3px); color: var(--cyan); }
  .upload input {
    position: absolute; inset: 0;
    opacity: 0; cursor: pointer; width: 100%; height: 100%;
  }
  .upload-icon {
    font-size: 26px; display: block;
    color: rgba(0,229,195,0.45); margin-bottom: 9px;
    transition: transform .25s, color .25s;
  }
  .upload-main { font-size: 14px; font-weight: 600; color: var(--text); display: block; }
  .upload-hint {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px; color: var(--muted);
    margin-top: 4px; display: block;
  }

  /* Toast notification */
  .toast {
    position: fixed; bottom: 28px; left: 50%; transform: translateX(-50%) translateY(80px);
    background: rgba(14,14,26,0.96);
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 12px; padding: 12px 22px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px; color: var(--text);
    backdrop-filter: blur(20px);
    transition: transform 0.35s cubic-bezier(0.22,1,0.36,1), opacity 0.35s;
    opacity: 0; z-index: 100;
    white-space: nowrap;
  }
  .toast.show {
    transform: translateX(-50%) translateY(0);
    opacity: 1;
  }
  .toast.success { border-color: rgba(0,229,195,0.35); color: var(--cyan); }
  .toast.error   { border-color: rgba(255,45,107,0.35); color: var(--pink); }

  .btns { display: flex; flex-direction: column; gap: 9px; }

  .btn {
    width: 100%; padding: 14px 20px;
    border-radius: 11px; border: none; cursor: pointer;
    font-family: 'Syne', sans-serif; font-size: 14px; font-weight: 600;
    display: flex; align-items: center; justify-content: center; gap: 9px;
    transition:
      transform .18s cubic-bezier(0.22,1,0.36,1),
      box-shadow .18s ease,
      background .18s ease,
      border-color .18s ease,
      opacity .18s ease;
    position: relative; overflow: hidden;
  }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none !important; }
  .btn::after {
    content: ''; position: absolute; inset: 0;
    background: rgba(255,255,255,0); transition: background .2s;
  }
  .btn:hover:not(:disabled)::after { background: rgba(255,255,255,0.07); }
  .btn:active:not(:disabled) { transform: scale(0.975) !important; }

  .btn-primary {
    background: linear-gradient(135deg, #ff2d6b 0%, #d91e7c 100%);
    color: #fff; box-shadow: 0 4px 22px rgba(255,45,107,0.38);
  }
  .btn-primary:hover:not(:disabled) {
    transform: translateY(-2px);
    box-shadow: 0 10px 36px rgba(255,45,107,0.54);
  }

  .btn-secondary {
    background: rgba(255,255,255,0.05);
    color: var(--text);
    border: 1px solid var(--border);
  }
  .btn-secondary:hover:not(:disabled) {
    transform: translateY(-2px);
    background: rgba(255,255,255,0.085);
    border-color: rgba(255,255,255,0.13);
    box-shadow: 0 6px 20px rgba(0,0,0,0.35);
  }

  .btn-ghost {
    background: transparent; color: var(--muted);
    border: 1px solid rgba(255,255,255,0.06);
  }
  .btn-ghost:hover:not(:disabled) { transform: translateY(-1px); color: var(--text); border-color: rgba(255,255,255,0.13); }

  .btn-icon { font-size: 15px; flex-shrink: 0; }
  .btn-arrow { margin-left: auto; opacity: 0.6; font-size: 13px; }

  .spinner {
    width: 14px; height: 14px;
    border: 2px solid rgba(255,255,255,0.25);
    border-top-color: #fff;
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
    flex-shrink: 0;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  .footer {
    margin-top: 34px; padding-top: 22px;
    border-top: 1px solid rgba(255,255,255,0.05);
    display: flex; align-items: center; justify-content: space-between;
    animation: up 0.5s 0.50s both;
  }
  .status {
    display: flex; align-items: center; gap: 7px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px; color: var(--muted);
  }
  .status-dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: var(--cyan); box-shadow: 0 0 8px var(--cyan);
    animation: blink 2.8s ease-in-out infinite;
  }
  .ver { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: rgba(255,255,255,0.13); }

  @keyframes up {
    from { opacity:0; transform: translateY(14px); }
    to   { opacity:1; transform: translateY(0); }
  }

  @media (max-width: 540px) {
    .card { padding: 36px 26px 36px; }
    .rule { margin: 0 -26px 28px; }
  }
`

// ─── helpers ────────────────────────────────────────────────────────────────

const ALLOWED_MIME = [
  "text/csv",
  "text/plain",                                                           // some browsers send CSV as text/plain
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",   // .xlsx
  "application/vnd.ms-excel"                                              // .xls
]

const ALLOWED_EXT = [".csv", ".xlsx", ".xls"]

function isValidFile(file) {
  const extOk = ALLOWED_EXT.some(ext => file.name.toLowerCase().endsWith(ext))
  const mimeOk = ALLOWED_MIME.includes(file.type)
  return extOk || mimeOk   // accept if either check passes (browser MIME can be wrong)
}

// ─── component ──────────────────────────────────────────────────────────────

export default function LandingPage({ user }) {
  const [dragging, setDragging]           = useState(false)
  const [uploadLoading, setUploadLoading] = useState(false)
  const [gmailLoading, setGmailLoading]   = useState(false)
  const [fetchLoading, setFetchLoading]   = useState(false)
  const [toast, setToast]                 = useState({ msg: '', type: '', show: false })

  // ── toast helper ──────────────────────────────────────────────────────────
  function showToast(msg, type = 'success') {
    setToast({ msg, type, show: true })
    setTimeout(() => setToast(t => ({ ...t, show: false })), 3000)
  }

  // ── get session token ─────────────────────────────────────────────────────
  async function getToken() {
    const { data, error } = await supabase.auth.getSession()
    if (error || !data.session) throw new Error("Not authenticated")
    return data.session.access_token
  }

  // ── FILE UPLOAD (CSV / Excel) ─────────────────────────────────────────────
  // FIX: removed the stray window.location.href redirect that was here before.
  // FIX: added extension-based validation as a fallback for unreliable browser MIME types.
  const handleFileUpload = async (e) => {
    const file = e.target.files[0]
    if (!file) return

    if (!isValidFile(file)) {
      showToast("Please upload a CSV or Excel file (.csv, .xlsx, .xls)", "error")
      return
    }

    setUploadLoading(true)
    try {
      const token = await getToken()

      const formData = new FormData()
      formData.append("file", file)

      const res = await fetch("http://127.0.0.1:8000/upload-file", {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
        body: formData
      })

      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || `Server error ${res.status}`)
      }

      const result = await res.json()
      showToast(`Uploaded! ${result.transactions_found} transactions found`, "success")
    } catch (err) {
      console.error("Upload error:", err)
      showToast(err.message || "Upload failed", "error")
    } finally {
      setUploadLoading(false)
      // Reset file input so the same file can be re-uploaded if needed
      e.target.value = ""
    }
  }

  // ── CONNECT GMAIL ─────────────────────────────────────────────────────────
  // FIX: now correctly fetches the token before redirecting.
  // NOTE: passing token in query string is a known limitation — see backend notes.
  const connectGmail = async () => {
    setGmailLoading(true)
    try {
      const token = await getToken()
      // Redirect to backend which will initiate Google OAuth flow.
      // The token is passed so the callback can identify the user.
      window.location.href = `http://127.0.0.1:8000/auth/google?token=${token}`
    } catch (err) {
      console.error("Gmail connect error:", err)
      showToast("Failed to start Gmail connection", "error")
      setGmailLoading(false)
    }
  }

  // ── FETCH GMAIL DATA ──────────────────────────────────────────────────────
  const handleGmailFetch = async () => {
    setFetchLoading(true)
    try {
      const token = await getToken()

      const res = await fetch("http://127.0.0.1:8000/fetch-gmail", {
        headers: { Authorization: `Bearer ${token}` }
      })

      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || err.error || `Server error ${res.status}`)
      }

      const result = await res.json()

      if (result.error) throw new Error(result.error)

      showToast(`Fetched ${result.transactions_found} transactions`, "success")
    } catch (err) {
      console.error("Gmail fetch error:", err)
      showToast(err.message || "Gmail fetch failed", "error")
    } finally {
      setFetchLoading(false)
    }
  }

  // ── TEST BACKEND AUTH ─────────────────────────────────────────────────────
  const testBackend = async () => {
    try {
      const token = await getToken()

      const res = await fetch("http://127.0.0.1:8000/protected", {
        headers: { Authorization: `Bearer ${token}` }
      })

      const result = await res.json()
      console.log("Backend auth result:", result)
      showToast(`Auth OK — ${result.email}`, "success")
    } catch (err) {
      console.error("Backend test error:", err)
      showToast("Backend auth failed", "error")
    }
  }

  return (
    <>
      <style>{CSS}</style>

      {/* Toast */}
      <div className={`toast ${toast.type} ${toast.show ? 'show' : ''}`}>
        {toast.msg}
      </div>

      <div className="lp">
        <div className="orb orb-1" />
        <div className="orb orb-2" />
        <div className="orb orb-3" />

        <div className="card">
          <div className="pill">
            <span className="pill-dot" />
            Financial Intelligence Engine
          </div>

          <h1 className="title">
            Welcome back,<br />
            <span className="grad">Mission Control</span>
          </h1>
          <p className="email">{user.email}</p>

          <hr className="rule" />

          {/* FILE UPLOAD */}
          <div className="sec">
            <p className="label">// Import Data</p>
            <div
              className={`upload${dragging ? ' drag' : ''}`}
              onDragEnter={() => setDragging(true)}
              onDragLeave={() => setDragging(false)}
              onDrop={() => setDragging(false)}
            >
              <input
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={handleFileUpload}
                disabled={uploadLoading}
              />
              {uploadLoading
                ? <span className="upload-icon" style={{ fontSize: 18 }}>⏳</span>
                : <span className="upload-icon">↑</span>
              }
              <span className="upload-main">
                {uploadLoading ? "Uploading…" : "Drop CSV or Excel file"}
              </span>
              <span className="upload-hint">.csv · .xlsx · .xls — click or drag to upload</span>
            </div>
          </div>

          {/* GMAIL */}
          <div className="sec">
            <p className="label">// Gmail Integration</p>
            <div className="btns">
              <button className="btn btn-primary" onClick={connectGmail} disabled={gmailLoading}>
                {gmailLoading
                  ? <span className="spinner" />
                  : <span className="btn-icon">✉</span>
                }
                {gmailLoading ? "Connecting…" : "Connect Gmail"}
                {!gmailLoading && <span className="btn-arrow">→</span>}
              </button>
              <button className="btn btn-secondary" onClick={handleGmailFetch} disabled={fetchLoading}>
                {fetchLoading
                  ? <span className="spinner" />
                  : <span className="btn-icon">↓</span>
                }
                {fetchLoading ? "Fetching…" : "Fetch Gmail Data"}
              </button>
            </div>
          </div>

          {/* DEVELOPER */}
          <div className="sec">
            <p className="label">// Developer</p>
            <button className="btn btn-ghost" onClick={testBackend}>
              <span className="btn-icon">⬡</span>
              Test Backend Auth
            </button>
          </div>

          <div className="footer">
            <span className="status">
              <span className="status-dot" />
              All systems operational
            </span>
            <span className="ver">v2.4.2</span>
          </div>
        </div>
      </div>
    </>
  )
}