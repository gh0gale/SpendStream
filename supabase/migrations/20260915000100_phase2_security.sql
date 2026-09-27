-- Phase 2 security (docs/IMPROVEMENTS.md items 2.1, 2.4, 2.5, 2.6, 2.10).
-- Tested locally by supabase/tests/security_test.sql.

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.5 Gmail tokens leave gmail_sync, which the browser reads
-- ─────────────────────────────────────────────────────────────────────────────

create table public.gmail_credentials (
    user_id                  uuid        primary key references auth.users(id) on delete cascade,
    access_token             text,
    refresh_token_encrypted  text,        -- Fernet ciphertext; key is TOKEN_ENCRYPTION_KEY on the backend
    updated_at               timestamptz not null default now()
);

-- Existing plaintext tokens cannot be encrypted from SQL, so they are dropped
-- and those users reconnect Gmail once. Their gmail_sync rows go too, so the
-- dashboard offers "Connect Gmail" instead of a connection that cannot sync.
delete from public.gmail_sync;
alter table public.gmail_sync
    drop column access_token,
    drop column refresh_token;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.10 Manual sync rate limit (one per user per 10 minutes, enforced in main.py)
-- ─────────────────────────────────────────────────────────────────────────────

alter table public.gmail_sync add column last_sync_requested_at timestamptz;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.4 One-time OAuth state values, so no login token travels in a URL
-- ─────────────────────────────────────────────────────────────────────────────

create table public.oauth_states (
    state       text        primary key,
    user_id     uuid        not null references auth.users(id) on delete cascade,
    created_at  timestamptz not null default now(),
    expires_at  timestamptz not null
);
create index oauth_states_expires_at_idx on public.oauth_states(expires_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.6 Row-level security on every table
--
-- The browser holds only the anon key and the user's JWT. It may read its own
-- rows from three tables and write nothing directly; corrections go through
-- correct_category() below. The backend uses the service role, which bypasses
-- row-level security. Supabase grants every new public table to anon and
-- authenticated by default, so a table added later needs the same treatment.
-- ─────────────────────────────────────────────────────────────────────────────

alter table public.transactions         enable row level security;
alter table public.bronze_transactions  enable row level security;
alter table public.silver_transactions  enable row level security;
alter table public.gold_monthly_summary enable row level security;
alter table public.gmail_sync           enable row level security;
alter table public.category_feedback    enable row level security;
alter table public.gmail_credentials    enable row level security;
alter table public.oauth_states         enable row level security;

revoke all on
    public.transactions,
    public.bronze_transactions,
    public.silver_transactions,
    public.gold_monthly_summary,
    public.gmail_sync,
    public.category_feedback,
    public.gmail_credentials,
    public.oauth_states
from anon, authenticated;

grant select on
    public.silver_transactions,
    public.gold_monthly_summary,
    public.gmail_sync
to authenticated;

create policy "Users read their own transactions"
    on public.silver_transactions for select to authenticated
    using (user_id = (select auth.uid()));

create policy "Users read their own monthly totals"
    on public.gold_monthly_summary for select to authenticated
    using (user_id = (select auth.uid()));

create policy "Users read their own Gmail status"
    on public.gmail_sync for select to authenticated
    using (user_id = (select auth.uid()));

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.1 Corrections run as the signed-in user (fixes SEC-03)
-- ─────────────────────────────────────────────────────────────────────────────

-- Recompute one monthly total from silver. Internal: not callable by clients.
create function public.refresh_gold_row(p_user_id uuid, p_month date, p_category text)
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_total numeric;
    v_count integer;
begin
    select coalesce(sum(amount), 0), count(*)
      into v_total, v_count
      from public.silver_transactions
     where user_id = p_user_id
       and is_categorised
       and category = p_category
       and transaction_date >= p_month
       and transaction_date < (p_month + interval '1 month');

    if v_count = 0 then
        delete from public.gold_monthly_summary
         where user_id = p_user_id and month = p_month and category = p_category;
    else
        insert into public.gold_monthly_summary
               (user_id, month, category, total_amount, txn_count, updated_at)
        values (p_user_id, p_month, p_category, round(v_total, 2), v_count, now())
        on conflict (user_id, month, category) do update
           set total_amount = excluded.total_amount,
               txn_count    = excluded.txn_count,
               updated_at   = excluded.updated_at;
    end if;
end;
$$;

revoke execute on function public.refresh_gold_row(uuid, date, text) from public, anon, authenticated;

-- Correct one of the caller's own transactions. The user id comes from the JWT,
-- never from the request. Updates the row, records the correction in
-- category_feedback, and refreshes the affected monthly totals, in one
-- transaction. Never touches the model.
create function public.correct_category(p_silver_id uuid, p_category text)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_user_id  uuid := auth.uid();
    v_row      public.silver_transactions%rowtype;
    v_raw_text text;
    v_month    date;
begin
    if v_user_id is null then
        raise exception 'Not signed in' using errcode = '28000';
    end if;

    -- Must match CATEGORIES in backend/ml/categoriser.py
    if p_category is null or p_category not in (
        'Education', 'Entertainment', 'Food', 'Groceries', 'Health',
        'Investment', 'Payments', 'Shopping', 'Subscription',
        'Transfer', 'Transport', 'Utilities', 'Other'
    ) then
        raise exception 'Unknown category: %', p_category using errcode = '22023';
    end if;

    select * into v_row
      from public.silver_transactions
     where id = p_silver_id and user_id = v_user_id
       for update;

    if not found then
        raise exception 'Transaction not found' using errcode = 'P0002';
    end if;

    update public.silver_transactions
       set category = p_category, is_categorised = true, user_corrected = true
     where id = v_row.id;

    select receiver into v_raw_text
      from public.bronze_transactions
     where id = v_row.bronze_id;

    insert into public.category_feedback
           (user_id, silver_id, merchant, raw_text, original_category,
            corrected_category, amount, corrected_at)
    values (v_user_id, v_row.id, v_row.merchant, coalesce(v_raw_text, v_row.merchant),
            v_row.category, p_category, v_row.amount, now())
    on conflict (user_id, silver_id) do update
       set merchant           = excluded.merchant,
           raw_text           = excluded.raw_text,
           original_category  = excluded.original_category,
           corrected_category = excluded.corrected_category,
           amount             = excluded.amount,
           corrected_at       = excluded.corrected_at;

    v_month := date_trunc('month', coalesce(v_row.transaction_date, current_date))::date;
    if v_row.category is not null and v_row.category <> p_category then
        perform public.refresh_gold_row(v_user_id, v_month, v_row.category);
    end if;
    perform public.refresh_gold_row(v_user_id, v_month, p_category);

    return jsonb_build_object(
        'ok',        true,
        'silver_id', v_row.id,
        'category',  p_category,
        'month',     v_month
    );
end;
$$;

revoke execute on function public.correct_category(uuid, text) from public, anon;
grant execute on function public.correct_category(uuid, text) to authenticated;
