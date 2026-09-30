import { createContext, useContext } from 'react'

// One spending read per signed-in visit (AppLayout provides it), so moving
// between Dashboard and Transactions does not aggregate the month totals again.
// It reloads when a sync finishes or a correction is saved (REVIEW_CHANGED).
export const SpendingContext = createContext(null)

export function useSpendingData() {
  const spending = useContext(SpendingContext)
  if (!spending) throw new Error('useSpendingData must be used inside AppLayout')
  return spending
}
