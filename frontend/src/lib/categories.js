// The category vocabulary. Must match CATEGORIES in backend/ml/features.py and
// the list inside correct_category() in supabase/migrations/ (contract in
// .claude/rules/general.md). Alphabetical, Other last, as the picker shows it.
export const CATEGORIES = [
  'Education', 'Entertainment', 'Food', 'Groceries', 'Health',
  'Investment', 'Payments', 'Shopping', 'Subscription',
  'Transfer', 'Transport', 'Utilities', 'Other',
]

// One-line meanings for the categories people confuse.
export const CATEGORY_HINTS = {
  Payments: 'Bills and dues paid to a company: card bills, loan EMIs, insurance.',
  Transfer: 'Money sent to a person, or to your own other account.',
  Other:    'You know what it was, and none of the categories fits.',
}

// A category filter value for rows the model was not sure about.
export const UNSURE = 'Unsure'
