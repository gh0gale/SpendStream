-- Phase 4: model versioning and quality monitoring
-- (docs/IMPROVEMENTS.md items 4.10 and 4.13).
--
-- Both are backend-only: no browser ever reads or writes either table, and
-- neither function is callable by a signed-in user.
--
-- Tested locally by supabase/tests/security_test.sql via run_local.sh.

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.10 A model is a row, not a file in git
--
-- The artifact itself lives in a private Supabase Storage bucket; this table
-- records which version is live, where it is, and what it scored. Rolling
-- back is then a one-row update rather than a redeploy, and the checksum lets
-- the server refuse a file that does not match what was promoted.
-- ─────────────────────────────────────────────────────────────────────────────

create table public.model_registry (
    version       text        primary key,        -- e.g. '2026-09-16T10:30:00Z'
    storage_path  text        not null,           -- path inside the private bucket
    sha256        text        not null,           -- verified before the file is loaded
    metrics       jsonb,                          -- macro-F1 and per-category scores (4.8)
    status        text        not null default 'candidate'
                              check (status in ('candidate', 'active', 'rolled_back', 'rejected')),
    notes         text,
    trained_at    timestamptz,
    activated_at  timestamptz,
    created_at    timestamptz not null default now()
);

-- At most one model is live at any moment. A partial unique index makes that
-- an invariant of the database rather than a convention the code must keep.
create unique index model_registry_one_active
    on public.model_registry((status)) where status = 'active';

create index model_registry_created_idx on public.model_registry(created_at desc);

alter table public.model_registry enable row level security;
revoke all on public.model_registry from anon, authenticated;

-- Promote one version and retire whatever was live, in one transaction, so
-- there is never a moment with two active models or none.
create function public.activate_model(p_version text)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_previous text;
begin
    if not exists (select 1 from public.model_registry where version = p_version) then
        raise exception 'Unknown model version: %', p_version using errcode = 'P0002';
    end if;

    select version into v_previous
      from public.model_registry
     where status = 'active';

    update public.model_registry
       set status = 'rolled_back'
     where status = 'active';

    update public.model_registry
       set status = 'active', activated_at = now()
     where version = p_version;

    return jsonb_build_object(
        'ok',       true,
        'active',   p_version,
        'previous', v_previous
    );
end;
$$;

revoke execute on function public.activate_model(text) from public, anon, authenticated;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.13 Quality monitoring
--
-- The number that matters is repeat_corrections: the same user correcting the
-- same merchant twice. That is the direct measure of "never correct the same
-- merchant twice", and it should stay at 0. Anything above 0 means a rule was
-- saved but did not take, and is worth investigating before anything else.
-- ─────────────────────────────────────────────────────────────────────────────

create function public.quality_report(p_since timestamptz default now() - interval '7 days')
returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
    with new_rows as (
        select count(*)::numeric                                     as total,
               count(*) filter (where not is_categorised)::numeric   as uncategorised
          from public.silver_transactions
         where created_at >= p_since
    ),
    corrections as (
        select count(*)::numeric as made
          from public.category_feedback
         where corrected_at >= p_since
    ),
    repeats as (
        select count(*)::numeric                as merchants_corrected_twice,
               coalesce(sum(corrections - 1), 0)::numeric as extra_corrections
          from public.user_merchant_rules
         where corrections > 1
    )
    select jsonb_build_object(
        'since',                     p_since,
        'transactions',              n.total,
        'uncategorised',             n.uncategorised,
        -- NULL rather than a made-up 0 when there were no transactions at all.
        'uncategorised_share',       round(n.uncategorised / nullif(n.total, 0), 4),
        'corrections',               c.made,
        'corrections_per_transaction', round(c.made / nullif(n.total, 0), 4),
        -- Should be 0. Above 0 means a correction had to be repeated.
        'repeat_corrections',        r.merchants_corrected_twice,
        'extra_corrections',         r.extra_corrections,
        'user_rules',                (select count(*) from public.user_merchant_rules),
        'directory_entries',         (select count(*) from public.merchant_directory),
        'active_model',              (select version from public.model_registry where status = 'active')
    )
      from new_rows n, corrections c, repeats r;
$$;

revoke execute on function public.quality_report(timestamptz) from public, anon, authenticated;
