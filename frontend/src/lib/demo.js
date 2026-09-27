// Real data behind the public demos, written by backend/export_demo.py from
// the developer's own account. While the file is absent this is null and the
// demo sections are left out (design rule 18: never a mockup).
const files = import.meta.glob('../data/demo-export.json', { eager: true, import: 'default' })

export const demo = Object.values(files)[0] ?? null
