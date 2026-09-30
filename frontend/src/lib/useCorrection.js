import { useState } from 'react'
import { supabase } from './supabase'
import { notifyReviewChanged } from './reviewQueue'
import { track } from './track'

// correct_category() runs in Postgres as the signed-in user: it updates the
// row, saves the choice as the user's rule for that merchant, and applies it
// to their other payments from the same merchant. It returns
// { ok, silver_id, category, also_updated, month } (contract in general.md).
export function useCorrection() {
  const [saving, setSaving] = useState(null)   // the category being saved

  // source: which page the correction came from ('transactions' | 'review').
  const correct = async (row, category, source) => {
    setSaving(category)
    try {
      const { data, error } = await supabase.rpc('correct_category', {
        p_silver_id: row.id,
        p_category: category,
      })
      if (error) throw new Error(error.message)
      notifyReviewChanged()
      const alsoUpdated = data?.also_updated ?? 0
      track('category_corrected', { also_updated: alsoUpdated, from: source, was_unsure: !row.category })
      return { alsoUpdated }
    } finally {
      setSaving(null)
    }
  }

  return { saving, correct }
}
