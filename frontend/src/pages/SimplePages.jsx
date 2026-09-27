import { Link } from 'react-router'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Reading.module.css'

export function Deleted() {
  usePageTitle('Account deleted')
  return (
    <article className={`page ${styles.article}`}>
      <header className={styles.head}>
        <h1>Your account and data are deleted</h1>
        <p>
          SpendStream asked Google to revoke its access to your Gmail. You can confirm it is gone on
          your <a href="https://myaccount.google.com/permissions">Google account's third-party access page</a>.
        </p>
        <Link to="/" className="link-arrow">Back to the home page</Link>
      </header>
    </article>
  )
}

export function NotFound() {
  usePageTitle('Page not found')
  return (
    <article className={`page ${styles.article}`}>
      <header className={styles.head}>
        <h1>There is no page here</h1>
        <Link to="/" className="link-arrow">Go to the home page</Link>
      </header>
    </article>
  )
}
