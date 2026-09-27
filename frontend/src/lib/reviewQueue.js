// A correction changes how many payments wait in Needs review; pages announce
// it so the count in the app navigation reloads.
export const REVIEW_CHANGED = 'spendstream:review-changed'

export const notifyReviewChanged = () => window.dispatchEvent(new Event(REVIEW_CHANGED))
