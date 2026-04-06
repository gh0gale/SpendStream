import os

categoriser_path = "ml/categoriser.py"
tasks_path = "tasks.py"
routes_path = "ml/correction_routes.py"

# 1. Update categoriser.py
with open(categoriser_path, "r", encoding="utf-8") as f:
    cat_code = f.read()

# Add supabase import and init
supabase_init = """
from supabase import create_client
import os

supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)
"""
if "supabase_admin = create_client" not in cat_code:
    cat_code = cat_code.replace("import joblib", f"import joblib\n{supabase_init}")

# Replace HistoryStore and OverrideStore with DB-aware classes
new_stores = """
@dataclass
class UserOverrideStore:
    _overrides: dict = field(default_factory=dict)

    def set(self, user_id: str, receiver: str, category: str):
        self._overrides[(_norm(user_id), _norm(receiver))] = category

    def get(self, user_id: str, receiver: str) -> Optional[str]:
        return self._overrides.get((_norm(user_id), _norm(receiver)))

@dataclass
class HistoryStore:
    _entries: dict = field(default_factory=lambda: defaultdict(list))
    _last:    dict = field(default_factory=dict)
    
    def record(self, user_id: str, receiver: str, category: str):
        key = (_norm(user_id), _norm(receiver))
        self._entries[key].append((category, time.time()))
        self._last[key] = category

    def get(self, user_id: str, receiver: str) -> tuple[dict, Optional[str]]:
        key     = (_norm(user_id), _norm(receiver))
        entries = self._entries.get(key, [])
        if not entries:
            return {}, self._last.get(key)
        sorted_entries = sorted(entries, key=lambda x: x[1], reverse=True)
        weighted: dict[str, float] = {}
        for pos, (cat, _ts) in enumerate(sorted_entries):
            w = 0.85 ** pos # decay
            weighted[cat] = weighted.get(cat, 0.0) + w
        return weighted, self._last.get(key)

    def frequency(self, user_id: str, receiver: str) -> int:
        key = (_norm(user_id), _norm(receiver))
        return len(self._entries.get(key, []))

    def record_amount(self, user_id: str, receiver: str, amount: float):
        key = (_norm(user_id), _norm(receiver))
        if not hasattr(self, "_amounts"):
            self._amounts = defaultdict(list)
        self._amounts[key].append(amount)

    def get_amounts(self, user_id: str, receiver: str) -> list[float]:
        if not hasattr(self, "_amounts"):
            return []
        key = (_norm(user_id), _norm(receiver))
        return self._amounts.get(key, [])

def _load_history_from_db(user_ids: list[str]):
    \"\"\"
    Pulls recent category_feedback from Supabase to populate
    the _history_store and _override_store dynamically for the current predict batch!
    Fixes the cross-process Celery worker disconnect!
    \"\"\"
    if not user_ids: return
    
    unique_users = list(set(user_ids))
    try:
        res = supabase_admin.table("category_feedback") \\
            .select("user_id, merchant, corrected_category, amount, corrected_at") \\
            .in_("user_id", unique_users) \\
            .order("corrected_at", desc=True) \\
            .limit(1000) \\
            .execute()
        
        if not res.data:
            return
            
        # Repopulate singleton caches with DB truth
        _history_store._entries.clear()
        _history_store._last.clear()
        if hasattr(_history_store, "_amounts"):
            _history_store._amounts.clear()
        _override_store._overrides.clear()
        
        for row in reversed(res.data): # oldest to newest so newest is .last
            u = row.get("user_id", "")
            m = row.get("merchant", "")
            c = row.get("corrected_category", "")
            a = float(row.get("amount") or 0.0)
            
            _history_store.record(u, m, c)
            if a > 0:
                _history_store.record_amount(u, m, a)
            _override_store.set(u, m, c)
            
    except Exception as e:
        log.error(f"[DB History Load] Failed: {e}")
"""

# Replace the classes
import re
cat_code = re.sub(r"@dataclass\nclass UserOverrideStore:.*?def _norm\(s: str\) -> str:", new_stores + "\n\ndef _norm(s: str) -> str:", cat_code, flags=re.DOTALL)

# Inject _load_history_from_db into predict_full and predict_batch
cat_code = cat_code.replace("    _ensure_loaded()\n\n    uid = user_id or", "    _ensure_loaded()\n    _load_history_from_db([user_id])\n\n    uid = user_id or")
cat_code = cat_code.replace("    _ensure_loaded()\n    N          = len(raw_texts)", "    _ensure_loaded()\n    _load_history_from_db(user_ids)\n    N          = len(raw_texts)")

# Disable inline refit thread
cat_code = re.sub(r"        if count >= ONLINE_UPDATE_THRESHOLD and not self\._is_running:.*?                    self\._is_running = False\n\n            threading\.Thread\(target=run, daemon=True\)\.start\(\)", "", cat_code, flags=re.DOTALL)

# Expose refit dynamically inside categoriser.py
cat_code = cat_code.replace("def _online_refit():", "def _online_refit(bulk_samples=None):")
cat_code = cat_code.replace("samples = _online_buffer.drain()", "samples = bulk_samples or _online_buffer.drain()")
cat_code = cat_code.replace("def _load_model():", "import os.path\n_last_model_mtime = 0\n\ndef _load_model():\n    global _last_model_mtime")
cat_code = cat_code.replace("if _model_loaded:\n            return", "if _model_loaded and os.path.getmtime(MODEL_PATH) == _last_model_mtime:\n            return")
cat_code = cat_code.replace("        _model_loaded = True\n        log.info(f\"Model loaded from disk", "        _model_loaded = True\n        _last_model_mtime = os.path.getmtime(MODEL_PATH)\n        log.info(f\"Model loaded from disk")

with open(categoriser_path, "w", encoding="utf-8") as f:
    f.write(cat_code)


# 2. Update correction_routes.py
with open(routes_path, "r", encoding="utf-8") as f:
    routes_code = f.read()

tasks_import = "from tasks import trigger_online_refit_task\nfrom traceback import print_exc\n"
if "from tasks import" not in routes_code:
    routes_code = routes_code.replace("from ml.categoriser import", tasks_import + "from ml.categoriser import")

# After record_feedback, trigger celery
celery_trigger = """    try:
        trigger_online_refit_task.delay()
    except Exception as e:
        print_exc()

    return {"status": "ok", "corrected": body.corrected_category}
"""
routes_code = re.sub(r"    record_feedback\(.*?\)\s+return {\"status\": \"ok\", \"corrected\": body.corrected_category}", celery_trigger, routes_code, flags=re.DOTALL)

with open(routes_path, "w", encoding="utf-8") as f:
    f.write(routes_code)


# 3. Update tasks.py
with open(tasks_path, "r", encoding="utf-8") as f:
    tasks_code = f.read()

refit_task = """
@celery.task(name="tasks.trigger_online_refit_task")
def trigger_online_refit_task():
    log.info("[Task] Extracing DB feedback for online refit")
    # Fetch 50 most recent corrects
    res = supabase_admin.table("category_feedback").select("*").order("corrected_at", desc=True).limit(50).execute()
    if not res.data:
        return {"status": "no data"}
    
    samples = []
    from ml.categoriser import _online_refit, extract_metadata
    for row in res.data:
        meta = extract_metadata(float(row.get("amount") or 0.0), row.get("corrected_at"))
        samples.append({
            "raw_text": row.get("raw_text") or row.get("merchant", ""),
            "category": row.get("corrected_category"),
            "metadata": meta
        })
    
    _online_refit(bulk_samples=samples)
    log.info("[Task] Refit complete")
    return {"status": "ok", "refit_samples": len(samples)}
"""
if "trigger_online_refit_task" not in tasks_code:
    tasks_code += "\n" + refit_task

with open(tasks_path, "w", encoding="utf-8") as f:
    f.write(tasks_code)

print("Patch applied successfully.")
