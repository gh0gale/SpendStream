// Ink chip for a settled category, pencil chip for "Unsure". As a button it
// opens the picker: a 44px hit area around the visible chip.
export default function CategoryChip({ category, onClick, label }) {
  const chip = (
    <span className={`chip ${category ? '' : 'chip-unsure'}`}>{category || 'Unsure'}</span>
  )
  if (!onClick) return chip
  return (
    <button type="button" className="chip-button" onClick={onClick} aria-label={label}>
      {chip}
    </button>
  )
}
