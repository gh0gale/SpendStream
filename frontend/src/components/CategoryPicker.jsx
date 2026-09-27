import { useEffect, useRef } from 'react'
import { CATEGORIES, CATEGORY_HINTS } from '../lib/categories'
import styles from './CategoryPicker.module.css'

// The 13 categories in a native modal dialog: it traps focus, closes on Esc
// and returns focus to the control that opened it. A bottom sheet on phones.
export default function CategoryPicker({ merchant, current, saving, onPick, onClose }) {
  const ref = useRef(null)

  useEffect(() => {
    const dialog = ref.current
    dialog.showModal()
    return () => dialog.close()
  }, [])

  return (
    <dialog
      ref={ref}
      className="sheet"
      aria-labelledby="picker-title"
      onCancel={(e) => { e.preventDefault(); if (!saving) onClose() }}
      onClick={(e) => { if (e.target === ref.current && !saving) onClose() }}
    >
      <h2 id="picker-title" className={styles.title}>Category for {merchant}</h2>
      <div className={styles.grid}>
        {CATEGORIES.map(cat => {
          const isCurrent = cat === current
          const isSaving = saving === cat
          return (
            <div key={cat} className={styles.option}>
              <button
                type="button"
                className={`btn ${styles.choice} ${isCurrent ? styles.current : ''}`}
                aria-pressed={isCurrent}
                aria-describedby={CATEGORY_HINTS[cat] ? `hint-${cat}` : undefined}
                disabled={Boolean(saving)}
                onClick={() => onPick(cat)}
              >
                {isSaving ? 'Saving' : cat}
              </button>
              {CATEGORY_HINTS[cat] && <p id={`hint-${cat}`} className={styles.hint}>{CATEGORY_HINTS[cat]}</p>}
            </div>
          )
        })}
      </div>
      <button type="button" className="btn btn-text" onClick={onClose} disabled={Boolean(saving)}>
        Cancel
      </button>
    </dialog>
  )
}
