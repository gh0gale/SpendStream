-- Phase 3: Gmail pipeline correctness and speed
-- (docs/IMPROVEMENTS.md items 3.1, 3.3, 3.4, 3.5, 3.6, 3.7).
-- Tested locally by supabase/tests/security_test.sql and phase3_backfill_*.sql.

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.1 One raw row per Gmail message
-- Rows without a message id (from before 2026-09-15) are not constrained:
-- NULLs never conflict with each other.
-- ─────────────────────────────────────────────────────────────────────────────

alter table public.transactions
    add constraint transactions_user_message_key unique (user_id, message_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.5 Work queues: each pipeline stage picks up rows whose marker is NULL
-- ─────────────────────────────────────────────────────────────────────────────

alter table public.transactions        add column processed_at timestamptz;  -- copied (or found duplicate) into bronze
alter table public.bronze_transactions add column processed_at timestamptz;  -- copied into silver
alter table public.silver_transactions add column predicted_at timestamptz;  -- the model has tried to categorise it

-- Rows already downstream are done.
update public.transactions t
   set processed_at = now()
 where exists (select 1 from public.bronze_transactions b where b.raw_id = t.id);

update public.bronze_transactions b
   set processed_at = now()
 where exists (select 1 from public.silver_transactions s where s.bronze_id = b.id);

update public.silver_transactions
   set predicted_at = coalesce(created_at, now())
 where is_categorised;

create index transactions_pending_idx
    on public.transactions(user_id) where processed_at is null;
create index bronze_transactions_pending_idx
    on public.bronze_transactions(user_id) where processed_at is null;
create index silver_transactions_pending_idx
    on public.silver_transactions(user_id) where predicted_at is null and not is_categorised;

-- One silver row per bronze row, so a retried batch cannot duplicate a
-- transaction. Earlier runs could have created duplicates: keep one per bronze
-- row, preferring a row the user corrected, then the oldest.
delete from public.silver_transactions s
 using (
     select id,
            row_number() over (
                partition by bronze_id
                order by coalesce(user_corrected, false) desc, created_at asc nulls last, id
            ) as rn
       from public.silver_transactions
      where bronze_id is not null
 ) ranked
 where s.id = ranked.id
   and ranked.rn > 1;

alter table public.silver_transactions
    add constraint silver_transactions_bronze_id_key unique (bronze_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.4 Predictions are written in one call per batch
-- Backend only (service role). Skips rows the user corrected meanwhile, so a
-- prediction can never overwrite a correction.
-- ─────────────────────────────────────────────────────────────────────────────

create function public.apply_predictions(p_user_id uuid, p_rows jsonb)
returns integer
language sql
security definer
set search_path = ''
as $$
    with input as (
        select (r ->> 'id')::uuid as id,
               r ->> 'category'   as category
          from jsonb_array_elements(p_rows) as r
    ),
    updated as (
        update public.silver_transactions s
           set category       = i.category,
               is_categorised = (i.category is not null),
               predicted_at   = now()
          from input i
         where s.id = i.id
           and s.user_id = p_user_id
           and not coalesce(s.user_corrected, false)
        returning 1
    )
    select count(*)::integer from updated;
$$;

revoke execute on function public.apply_predictions(uuid, jsonb) from public, anon, authenticated;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.3 Gmail access that only the user can restore
-- ─────────────────────────────────────────────────────────────────────────────

alter table public.gmail_sync
    add column needs_reconnect boolean not null default false;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.6 Monthly totals are computed from silver, so they cannot go stale
-- ─────────────────────────────────────────────────────────────────────────────

-- correct_category() no longer maintains a totals table.
create or replace function public.correct_category(p_silver_id uuid, p_category text)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_user_id  uuid := auth.uid();
    v_row      public.silver_transactions%rowtype;
    v_raw_text text;
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

    return jsonb_build_object(
        'ok',        true,
        'silver_id', v_row.id,
        'category',  p_category,
        'month',     date_trunc('month', coalesce(v_row.transaction_date, current_date))::date
    );
end;
$$;

revoke execute on function public.correct_category(uuid, text) from public, anon;
grant execute on function public.correct_category(uuid, text) to authenticated;

drop function public.refresh_gold_row(uuid, date, text);

drop table public.gold_monthly_summary;

-- security_invoker: the view runs with the caller's rights, so the row-level
-- security policy on silver_transactions limits each user to their own totals.
create view public.gold_monthly_summary
with (security_invoker = true) as
select user_id,
       date_trunc('month', transaction_date)::date as month,
       category,
       round(sum(amount), 2)                         as total_amount,
       count(*)::integer                             as txn_count,
       max(created_at)                               as updated_at
  from public.silver_transactions
 where is_categorised
   and category is not null
   and transaction_date is not null
 group by user_id, date_trunc('month', transaction_date)::date, category;

revoke all on public.gold_monthly_summary from anon, authenticated;
grant select on public.gold_monthly_summary to authenticated;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.7 Every Gmail sync is recorded; the dashboard polls its row
-- ─────────────────────────────────────────────────────────────────────────────

create table public.sync_jobs (
    id                  uuid        primary key default gen_random_uuid(),
    user_id             uuid        not null references auth.users(id) on delete cascade,
    kind                text        not null check (kind in ('manual', 'scheduled')),
    runner              text        not null default 'api' check (runner in ('api', 'cron_runner')),
    status              text        not null default 'queued'
                                    check (status in ('queued', 'running', 'succeeded', 'failed')),
    error               text,                    -- shown to the user; no internal details
    transactions_found  integer,                 -- new raw rows this sync inserted
    created_at          timestamptz not null default now(),
    started_at          timestamptz,
    finished_at         timestamptz
);
create index sync_jobs_user_created_idx on public.sync_jobs(user_id, created_at desc);

alter table public.sync_jobs enable row level security;
revoke all on public.sync_jobs from anon, authenticated;
grant select on public.sync_jobs to authenticated;

create policy "Users read their own sync jobs"
    on public.sync_jobs for select to authenticated
    using (user_id = (select auth.uid()));
