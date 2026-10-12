// Whether this account closed the dashboard's getting-started guide. Kept per
// account in this browser only; blocked storage just shows the guide again.
const key = (userId) => `spendstream.guideHidden.${userId}`

export function isGuideHidden(userId) {
  try { return localStorage.getItem(key(userId)) === '1' } catch { return false }
}

export function setGuideHidden(userId, hidden) {
  try {
    if (hidden) localStorage.setItem(key(userId), '1')
    else localStorage.removeItem(key(userId))
  } catch { /* storage blocked: the choice lasts this visit only */ }
}
