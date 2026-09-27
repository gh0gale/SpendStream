-- Phase 4: corrections that stick, and stay where they belong
-- (docs/IMPROVEMENTS.md items 4.1, 4.2, 4.3, 4.4).
--
-- The requirement this file exists for: a user must never correct the same
-- merchant twice, and one person's correction must never move anyone else's
-- predictions. Those pull in opposite directions, so each correction lands in
-- exactly two places — the user's own rules, which are absolute for them, and
-- a shared directory that only listens to agreement between several people.
--
-- Tested locally by supabase/tests/security_test.sql via run_local.sh.

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.1 One stable key per merchant (DATA-05)
--
-- Mirror of merchant_key() in backend/merchant_identity.py, which computes the
-- key for every new row. This copy exists to backfill rows written before
-- 2026-09-16 and to key corrections inside correct_category(). The two must
-- agree: the cases they are both checked against are MERCHANT_KEY_CASES in
-- backend/test_merchant_identity.py, asserted for this function at the end of
-- security_test.sql.
-- ─────────────────────────────────────────────────────────────────────────────

create function public.merchant_key_of(p_receiver text, p_merchant text)
returns text
language sql
immutable
set search_path = ''
as $$
    select case
        -- A UPI payee id, lowercased. The handle must start with a letter, so
        -- an amount ("60@2") is not mistaken for one.
        when p_receiver ~ '[A-Za-z0-9][A-Za-z0-9._-]*@[A-Za-z][A-Za-z0-9.-]*'
        then 'upi:' || lower(
            (regexp_match(p_receiver, '[A-Za-z0-9][A-Za-z0-9._-]*@[A-Za-z][A-Za-z0-9.-]*'))[1]
        )
        -- Otherwise the cleaned merchant name, letters and digits only.
        else nullif(
            'name:' || regexp_replace(lower(coalesce(p_merchant, '')), '[^a-z0-9]+', '', 'g'),
            'name:'
        )
    end;
$$;

alter table public.silver_transactions
    add column merchant_key      text,
    -- Set by the pipeline from merchant_identity.looks_like_person(): a
    -- payment to a human being. Kept on the row so correct_category() can
    -- pass it on without re-deriving it in SQL, which cannot tell a person's
    -- UPI id from a shop's.
    add column merchant_is_person boolean not null default false;

-- Backfill: existing silver rows get a key from their bronze receiver, or
-- from the cleaned merchant name when the bronze row is gone.
update public.silver_transactions s
   set merchant_key = public.merchant_key_of(
           coalesce((select b.receiver
                       from public.bronze_transactions b
                      where b.id = s.bronze_id), s.merchant),
           s.merchant
       );

create index silver_transactions_user_merchant_idx
    on public.silver_transactions(user_id, merchant_key)
 where merchant_key is not null;

-- Corrections carry the key too, so the weekly retrain (4.11) can group them.
alter table public.category_feedback add column merchant_key text;

update public.category_feedback f
   set merchant_key = public.merchant_key_of(f.raw_text, f.merchant);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.2 A correction becomes the user's own rule
--
-- Absolute for the user who made it, invisible to everyone else. This is what
-- makes "never correct the same merchant twice" true: the rule is consulted
-- before the model on every future transaction, and it never expires or falls
-- out of a row window the way the old 1,000-row feedback lookup did.
-- ─────────────────────────────────────────────────────────────────────────────

create table public.user_merchant_rules (
    user_id       uuid        not null references auth.users(id) on delete cascade,
    merchant_key  text        not null,
    category      text        not null,
    is_person     boolean     not null default false,  -- kept out of the shared directory
    corrections   integer     not null default 1,      -- how many times the user has said this
    updated_at    timestamptz not null default now(),
    primary key (user_id, merchant_key)
);

alter table public.user_merchant_rules enable row level security;
revoke all on public.user_merchant_rules from anon, authenticated;
grant select on public.user_merchant_rules to authenticated;

create policy "Users read their own merchant rules"
    on public.user_merchant_rules for select to authenticated
    using (user_id = (select auth.uid()));

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.4 The shared merchant directory
--
-- Backend only: no client ever reads or writes it. An entry appears only when
-- several unrelated people independently agree, so one person correcting
-- Amazon to Groceries changes nothing for anyone but themselves.
-- ─────────────────────────────────────────────────────────────────────────────

create table public.merchant_directory (
    merchant_key  text        primary key,
    category      text        not null,
    source        text        not null check (source in ('curated', 'consensus')),
    user_count    integer     not null default 0,   -- distinct users who agreed
    agreement     numeric,                          -- share of users on this category
    updated_at    timestamptz not null default now()
);

alter table public.merchant_directory enable row level security;
revoke all on public.merchant_directory from anon, authenticated;

-- Thresholds for a merchant to become shared knowledge.
-- Three unrelated people, four out of five of them agreeing.
create function public.refresh_merchant_directory(
    p_min_users     integer default 3,
    p_min_agreement numeric default 0.80
)
returns integer
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_rows integer;
begin
    with counted as (
        select merchant_key,
               category,
               count(*)::numeric as votes
          from public.user_merchant_rules
         where not is_person          -- payments to people are never shared
         group by merchant_key, category
    ),
    totals as (
        select merchant_key, sum(votes) as total
          from counted
         group by merchant_key
    ),
    winners as (
        select c.merchant_key,
               c.category,
               c.votes::integer                as user_count,
               round(c.votes / t.total, 4)     as agreement
          from counted c
          join totals  t using (merchant_key)
         where t.total    >= p_min_users
           and c.votes / t.total >= p_min_agreement
    )
    insert into public.merchant_directory
           (merchant_key, category, source, user_count, agreement, updated_at)
    select merchant_key, category, 'consensus', user_count, agreement, now()
      from winners
    -- A curated entry is a deliberate decision and outranks consensus.
    on conflict (merchant_key) do update
       set category   = excluded.category,
           user_count = excluded.user_count,
           agreement  = excluded.agreement,
           updated_at = now()
     where public.merchant_directory.source = 'consensus';

    get diagnostics v_rows = row_count;

    -- A merchant that no longer has consensus stops being shared.
    delete from public.merchant_directory d
     where d.source = 'consensus'
       and not exists (
           select 1
             from public.user_merchant_rules r
            where r.merchant_key = d.merchant_key
              and not r.is_person
            group by r.merchant_key
           having count(*) >= p_min_users
       );

    return v_rows;
end;
$$;

revoke execute on function public.refresh_merchant_directory(integer, numeric)
    from public, anon, authenticated;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.2 correct_category(), third version
--
-- Now, in one transaction: update the row, record the feedback, save the
-- user's rule, and apply that rule to the user's other transactions from the
-- same merchant that they have not corrected by hand.
--
-- Returns `also_updated`: how many other rows moved. frontend/src/pages/
-- Transactions.jsx shows it, so the user can see the correction spread.
-- ─────────────────────────────────────────────────────────────────────────────

create or replace function public.correct_category(p_silver_id uuid, p_category text)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_user_id      uuid := auth.uid();
    v_row          public.silver_transactions%rowtype;
    v_raw_text     text;
    v_key          text;
    v_also_updated integer := 0;
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

    -- Rows written before 4.1 have no key; derive one now.
    v_key := coalesce(
        v_row.merchant_key,
        public.merchant_key_of(coalesce(v_raw_text, v_row.merchant), v_row.merchant)
    );

    insert into public.category_feedback
           (user_id, silver_id, merchant, merchant_key, raw_text, original_category,
            corrected_category, amount, corrected_at)
    values (v_user_id, v_row.id, v_row.merchant, v_key, coalesce(v_raw_text, v_row.merchant),
            v_row.category, p_category, v_row.amount, now())
    on conflict (user_id, silver_id) do update
       set merchant           = excluded.merchant,
           merchant_key       = excluded.merchant_key,
           raw_text           = excluded.raw_text,
           original_category  = excluded.original_category,
           corrected_category = excluded.corrected_category,
           amount             = excluded.amount,
           corrected_at       = excluded.corrected_at;

    if v_key is not null then
        -- The rule. corrections counts repeats, which should stay at 1: a
        -- second correction of the same merchant means the rule did not hold,
        -- and that is the number 4.13 reports on.
        insert into public.user_merchant_rules
               (user_id, merchant_key, category, is_person, corrections, updated_at)
        values (v_user_id, v_key, p_category, coalesce(v_row.merchant_is_person, false), 1, now())
        on conflict (user_id, merchant_key) do update
           set category    = excluded.category,
               is_person   = excluded.is_person,
               corrections = public.user_merchant_rules.corrections + 1,
               updated_at  = now();

        -- Apply it to this user's other transactions from the same merchant,
        -- leaving alone any row they corrected by hand themselves.
        with spread as (
            update public.silver_transactions s
               set category       = p_category,
                   is_categorised = true,
                   predicted_at   = now()
             where s.user_id      = v_user_id
               and s.merchant_key = v_key
               and s.id          <> v_row.id
               and not coalesce(s.user_corrected, false)
               and (s.category is distinct from p_category)
            returning 1
        )
        select count(*)::integer into v_also_updated from spread;
    end if;

    return jsonb_build_object(
        'ok',           true,
        'silver_id',    v_row.id,
        'category',     p_category,
        'also_updated', v_also_updated,
        'month',        date_trunc('month', coalesce(v_row.transaction_date, current_date))::date
    );
end;
$$;

revoke execute on function public.correct_category(uuid, text) from public, anon;
grant execute on function public.correct_category(uuid, text) to authenticated;
