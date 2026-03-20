import { supabase } from '../lib/supabase'

export default function LandingPage({ user }) {

  // ✅ FILE UPLOAD (CSV / EXCEL)
  const handleFileUpload = async (e) => {
    const file = e.target.files[0]

    if (!file) return

    // ✅ Validate file type
    const allowedTypes = [
      "text/csv",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel"
    ]

    if (!allowedTypes.includes(file.type)) {
      alert("Please upload a CSV or Excel file")
      return
    }

    const { data } = await supabase.auth.getSession()

    const formData = new FormData()
    formData.append("file", file)

    const res = await fetch("http://127.0.0.1:8000/upload-file", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${data.session.access_token}`
      },
      body: formData
    })

    const result = await res.json()
    console.log(result)

    alert(`Uploaded! Transactions found: ${result.transactions_found}`)
  }

  // 🔐 CONNECT GMAIL (NEW)
  const connectGmail = () => {
    window.location.href = "http://127.0.0.1:8000/auth/google"
  }

  // 📧 FETCH GMAIL DATA (UPDATED)
  const handleGmailFetch = async () => {
    const { data } = await supabase.auth.getSession()

    const res = await fetch("http://127.0.0.1:8000/fetch-gmail", {
      headers: {
        Authorization: `Bearer ${data.session.access_token}`
      }
    })

    const result = await res.json()
    console.log(result)

    alert(`Fetched ${result.transactions_found} transactions`)
  }

  // 🔐 TEST BACKEND AUTH (UNCHANGED)
  const testBackend = async () => {
    const { data } = await supabase.auth.getSession()

    const res = await fetch('http://127.0.0.1:8000/protected', {
      headers: {
        Authorization: `Bearer ${data.session.access_token}`
      }
    })

    const result = await res.json()
    console.log(result)
  }

  return (
    <div style={{ padding: 20 }}>
      <h2>Welcome</h2>
      <p>{user.email}</p>

      {/* ✅ FILE UPLOAD */}
      <div style={{ marginTop: 20 }}>
        <input 
          type="file" 
          accept=".csv, .xlsx, .xls"
          onChange={handleFileUpload} 
        />
      </div>

      {/* 🔐 CONNECT GMAIL */}
      <div style={{ marginTop: 10 }}>
        <button onClick={connectGmail}>
          Connect Gmail
        </button>
      </div>

      {/* 📧 FETCH GMAIL */}
      <div style={{ marginTop: 10 }}>
        <button onClick={handleGmailFetch}>
          Fetch Gmail Data
        </button>
      </div>

      {/* 🔧 TEST BACKEND */}
      <div style={{ marginTop: 10 }}>
        <button onClick={testBackend}>
          Test Backend Auth
        </button>
      </div>
    </div>
  )
}