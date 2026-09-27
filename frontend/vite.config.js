import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// strictPort: the backend allows exactly one CORS origin (FRONTEND_URL), so
// the dev server must fail loudly rather than move to another port.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true },
})
