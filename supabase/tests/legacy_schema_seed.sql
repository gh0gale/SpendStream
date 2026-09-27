-- Tables as they exist on projects created before 2026-09-15 (hand-made, not
-- from the baseline migration). Shape copied from the real dev project's
-- PostgREST schema on 2026-09-26: surrogate ids, no transactions.message_id,
-- no (user_id, silver_id) key on feedback, no user_id foreign keys, and a
-- fingerprint without a unique constraint (constraints are not visible
-- through PostgREST, so the weakest case is assumed).
-- run_local.sh applies: stub, this file, then every migration except the
-- baseline, then legacy_schema_checks.sql.

create table public.transactions (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null, amount numeric not null, receiver text,
    transaction_type text default 'debit', "timestamp" timestamptz, source text,
    raw_text text, created_at timestamptz default now()
);
create table public.bronze_transactions (
    id uuid primary key default gen_random_uuid(),
    raw_id uuid, user_id uuid not null, amount numeric, receiver text,
    transaction_type text, "timestamp" timestamptz, source text, raw_text text,
    fingerprint text, message_id text, is_duplicate boolean default false,
    created_at timestamptz default now()
);
create table public.silver_transactions (
    id uuid primary key default gen_random_uuid(),
    bronze_id uuid, user_id uuid not null, amount numeric, merchant text,
    transaction_type text, transaction_date date, source text, category text,
    is_categorised boolean default false, user_corrected boolean default false,
    created_at timestamptz default now()
);
create table public.gold_monthly_summary (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null, month date not null, category text not null,
    total_amount numeric, txn_count integer, updated_at timestamptz
);
create table public.gmail_sync (
    user_id uuid primary key, access_token text, refresh_token text,
    last_fetched timestamptz, updated_at timestamptz
);
create table public.category_feedback (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null, silver_id uuid not null, merchant text, raw_text text,
    original_category text, corrected_category text, amount numeric,
    corrected_at timestamptz
);

insert into auth.users (id, email) values
    ('44444444-4444-4444-4444-444444444444', 'd@example.test');

insert into public.transactions (id, user_id, amount, receiver, "timestamp", source) values
    ('d0000000-0000-0000-0000-000000000001', '44444444-4444-4444-4444-444444444444', 120, 'VPA swiggy@icici SWIGGY', '2026-08-01T10:00:00Z', 'gmail');
insert into public.bronze_transactions (id, raw_id, user_id, amount, receiver, "timestamp", source, fingerprint) values
    ('d1000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', '44444444-4444-4444-4444-444444444444', 120, 'VPA swiggy@icici SWIGGY', '2026-08-01T10:00:00Z', 'gmail', 'fp-d1');
insert into public.silver_transactions (id, bronze_id, user_id, amount, merchant, transaction_date, source, category, is_categorised, user_corrected) values
    ('d2000000-0000-0000-0000-000000000001', 'd1000000-0000-0000-0000-000000000001', '44444444-4444-4444-4444-444444444444', 120, 'Swiggy', '2026-08-01', 'gmail', 'Food', true, true);
insert into public.gmail_sync (user_id, access_token, refresh_token) values
    ('44444444-4444-4444-4444-444444444444', 'plain-old-token', 'plain-old-refresh');
-- The same row corrected twice: the older one must go, the newer must stay.
insert into public.category_feedback (user_id, silver_id, merchant, corrected_category, corrected_at) values
    ('44444444-4444-4444-4444-444444444444', 'd2000000-0000-0000-0000-000000000001', 'Swiggy', 'Groceries', '2026-08-02T10:00:00Z'),
    ('44444444-4444-4444-4444-444444444444', 'd2000000-0000-0000-0000-000000000001', 'Swiggy', 'Food',      '2026-08-03T10:00:00Z');
