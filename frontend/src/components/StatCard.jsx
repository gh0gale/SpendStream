import styles from './StatCard.module.css'

export default function StatCard({ label, value, sub, accent, icon, loading }) {
  return (
    <div className={`${styles.card} ${accent ? styles.accent : ''}`}>
      {accent && <div className={styles.topBar} />}
      <div className={styles.header}>
        <p className={styles.label}>{label}</p>
        {icon && <span className={`${styles.icon} ${accent ? styles.gold : styles.muted}`}>{icon}</span>}
      </div>
      {loading ? (
        <div className={styles.skeleton} />
      ) : (
        <>
          <p className={`${styles.value} ${accent ? styles.accent : styles.normal}`}>{value}</p>
          {sub && <p className={styles.sub}>{sub}</p>}
        </>
      )}
    </div>
  )
}