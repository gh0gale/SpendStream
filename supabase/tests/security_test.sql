-- Security checks for the migrations.
-- Run with supabase/tests/run_local.sh, which applies local_auth_stub.sql and
-- every migration to a throwaway Postgres first. Each check raises an
-- exception on failure; psql runs with ON_ERROR_STOP, so any failure exits non-zero.
--
-- User A: 11111111-1111-1111-1111-111111111111
-- User B: 22222222-2222-2222-2222-222222222222

\set ON_ERROR_STOP on

-- ── Seed (as the table owner, like the backend's service role) ──────────────

insert into auth.users (id, email) values
    ('11111111-1111-1111-1111-111111111111', 'a@example.test'),
    ('22222222-2222-2222-2222-222222222222', 'b@example.test');

insert into public.transactions (id, user_id, amount, receiver, "timestamp", source, message_id) values
    ('a0000000-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111', 100, 'VPA zomato@hdfcbank ZOMATO', '2026-09-10T12:00:00Z', 'gmail', 'msg-a1'),
    ('b0000000-0000-0000-0000-000000000001', '22222222-2222-2222-2222-222222222222', 250, 'VPA uber@hdfcbank UBER',     '2026-09-11T09:00:00Z', 'gmail', 'msg-b1');

insert into public.bronze_transactions (id, raw_id, user_id, amount, receiver, "timestamp", source, fingerprint) values
    ('a1000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111', 100, 'VPA zomato@hdfcbank ZOMATO', '2026-09-10T12:00:00Z', 'gmail', 'fp-a1'),
    ('b1000000-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000001', '22222222-2222-2222-2222-222222222222', 250, 'VPA uber@hdfcbank UBER',     '2026-09-11T09:00:00Z', 'gmail', 'fp-b1');

insert into public.silver_transactions (id, bronze_id, user_id, amount, merchant, transaction_date, source, category, is_categorised) values
    ('a2000000-0000-0000-0000-000000000001', 'a1000000-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111', 100, 'Zomato', '2026-09-10', 'gmail', 'Food',      true),
    ('a2000000-0000-0000-0000-000000000002', null,                                   '11111111-1111-1111-1111-111111111111',  40, 'Zomato', '2026-09-12', 'gmail', 'Food',      true),
    ('b2000000-0000-0000-0000-000000000001', 'b1000000-0000-0000-0000-000000000001', '22222222-2222-2222-2222-222222222222', 250, 'Uber',   '2026-09-11', 'gmail', 'Transport', true);

insert into public.gmail_sync (user_id, last_fetched, updated_at) values
    ('11111111-1111-1111-1111-111111111111', now(), now()),
    ('22222222-2222-2222-2222-222222222222', now(), now());

insert into public.gmail_credentials (user_id, access_token, refresh_token_encrypted) values
    ('11111111-1111-1111-1111-111111111111', 'access-a', 'ciphertext-a'),
    ('22222222-2222-2222-2222-222222222222', 'access-b', 'ciphertext-b');

insert into public.oauth_states (state, user_id, expires_at) values
    ('state-a', '11111111-1111-1111-1111-111111111111', now() + interval '10 minutes');

insert into public.sync_jobs (user_id, kind, status) values
    ('11111111-1111-1111-1111-111111111111', 'manual',    'succeeded'),
    ('22222222-2222-2222-2222-222222222222', 'scheduled', 'running');

-- ── 2.5: gmail_sync no longer holds tokens ──────────────────────────────────

do $$
begin
    assert not exists (
        select 1 from information_schema.columns
         where table_schema = 'public' and table_name = 'gmail_sync'
           and column_name in ('access_token', 'refresh_token')
    ), 'gmail_sync still has token columns';
end $$;

-- ── 3.1 and 3.5: uniqueness the pipeline relies on ──────────────────────────

do $$
begin
    begin
        insert into public.transactions (user_id, amount, message_id)
        values ('11111111-1111-1111-1111-111111111111', 1, 'msg-a1');
        raise exception 'a second raw row for the same Gmail message was accepted';
    exception when unique_violation then null;
    end;

    -- Rows without a message id (pre-2026-09-15) are not constrained
    insert into public.transactions (user_id, amount) values
        ('11111111-1111-1111-1111-111111111111', 1),
        ('11111111-1111-1111-1111-111111111111', 1);
    delete from public.transactions
     where user_id = '11111111-1111-1111-1111-111111111111' and message_id is null;

    begin
        insert into public.silver_transactions (bronze_id, user_id, amount)
        values ('a1000000-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111', 1);
        raise exception 'a second silver row for the same bronze row was accepted';
    exception when unique_violation then null;
    end;
end $$;

-- ── 2.6 and 3.6: signed-in user A reads only their own rows ─────────────────

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
begin
    assert (select count(*) from public.silver_transactions)  = 2, 'A should see exactly 2 silver rows';
    assert (select count(*) from public.gold_monthly_summary) = 1, 'A should see exactly 1 monthly total';
    assert (select total_amount from public.gold_monthly_summary where category = 'Food') = 140,
        'A''s Food total should be 140';
    assert (select txn_count from public.gold_monthly_summary where category = 'Food') = 2,
        'A''s Food count should be 2';
    assert (select count(*) from public.gmail_sync) = 1, 'A should see exactly 1 gmail_sync row';
    assert (select needs_reconnect from public.gmail_sync) = false, 'A should be able to read needs_reconnect';
    assert (select count(*) from public.sync_jobs) = 1, 'A should see exactly 1 sync job';
    assert not exists (select 1 from public.silver_transactions  where user_id <> auth.uid()),
        'A can see another user''s transactions';
    assert not exists (select 1 from public.gold_monthly_summary where user_id <> auth.uid()),
        'A can see another user''s monthly totals';
    assert not exists (select 1 from public.sync_jobs            where user_id <> auth.uid()),
        'A can see another user''s sync jobs';
end $$;

-- DATA-07 (2026-09-27): failed message ids are kept for the next sync, and
-- only the backend writes them.
do $$
begin
    assert (select pending_message_ids from public.gmail_sync) = '{}',
        'pending_message_ids should default to an empty list';
    begin
        update public.gmail_sync set pending_message_ids = array['x'];
        assert false, 'A could write pending_message_ids';
    exception when insufficient_privilege then
        null;
    end;
end $$;

-- A cannot read the tables the browser has no business with
do $$
declare
    t text;
begin
    foreach t in array array['transactions', 'bronze_transactions', 'category_feedback',
                             'gmail_credentials', 'oauth_states'] loop
        begin
            execute format('select 1 from public.%I limit 1', t);
            raise exception 'authenticated can read public.%', t;
        exception when insufficient_privilege then
            null;
        end;
    end loop;
end $$;

-- A cannot write directly, even to their own rows
do $$
begin
    begin
        update public.silver_transactions set category = 'Other'
         where id = 'a2000000-0000-0000-0000-000000000001';
        raise exception 'authenticated can update silver directly';
    exception when insufficient_privilege then null;
    end;

    begin
        insert into public.category_feedback (user_id, silver_id, corrected_category)
        values (auth.uid(), 'a2000000-0000-0000-0000-000000000001', 'Other');
        raise exception 'authenticated can insert feedback directly';
    exception when insufficient_privilege then null;
    end;

    begin
        insert into public.sync_jobs (user_id, kind) values (auth.uid(), 'manual');
        raise exception 'authenticated can create sync jobs directly';
    exception when insufficient_privilege then null;
    end;

    begin
        perform public.apply_predictions(auth.uid(), '[]'::jsonb);
        raise exception 'authenticated can call apply_predictions';
    exception when insufficient_privilege then null;
    end;
end $$;

-- ── 2.1: A corrects their own transaction; totals follow (3.6) ──────────────

do $$
declare
    r jsonb;
begin
    r := public.correct_category('a2000000-0000-0000-0000-000000000001', 'Shopping');
    assert (r ->> 'ok')::boolean, 'correct_category did not return ok';
    assert r ->> 'month' = '2026-09-01', 'wrong month: ' || coalesce(r ->> 'month', 'null');

    assert (select category from public.silver_transactions
             where id = 'a2000000-0000-0000-0000-000000000001') = 'Shopping', 'silver row not updated';
    assert (select user_corrected from public.silver_transactions
             where id = 'a2000000-0000-0000-0000-000000000001'), 'user_corrected not set';

    assert (select total_amount from public.gold_monthly_summary
             where month = '2026-09-01' and category = 'Food') = 40, 'Food total should drop to 40';
    assert (select txn_count from public.gold_monthly_summary
             where month = '2026-09-01' and category = 'Food') = 1, 'Food count should drop to 1';
    assert (select total_amount from public.gold_monthly_summary
             where month = '2026-09-01' and category = 'Shopping') = 100, 'Shopping total should be 100';
end $$;

-- A cannot correct B's transaction, and unknown categories are refused
do $$
begin
    begin
        perform public.correct_category('b2000000-0000-0000-0000-000000000001', 'Food');
        raise exception 'A corrected B''s transaction';
    exception when sqlstate 'P0002' then null;
    end;

    begin
        perform public.correct_category('a2000000-0000-0000-0000-000000000002', 'NotACategory');
        raise exception 'an unknown category was accepted';
    exception when sqlstate '22023' then null;
    end;
end $$;

-- A JWT without a user id cannot correct anything
select set_config('request.jwt.claims', '{"role":"authenticated"}', false);
do $$
begin
    begin
        perform public.correct_category('a2000000-0000-0000-0000-000000000002', 'Food');
        raise exception 'correct_category ran without a user id';
    exception when sqlstate '28000' then null;
    end;
end $$;

-- ── 2.6: the anon key can do nothing ────────────────────────────────────────

set role anon;
select set_config('request.jwt.claims', '{"role":"anon"}', false);

do $$
declare
    t text;
begin
    foreach t in array array['silver_transactions', 'gold_monthly_summary', 'gmail_sync', 'sync_jobs'] loop
        begin
            execute format('select 1 from public.%I limit 1', t);
            raise exception 'anon can read public.%', t;
        exception when insufficient_privilege then
            null;
        end;
    end loop;

    begin
        perform public.correct_category('a2000000-0000-0000-0000-000000000001', 'Food');
        raise exception 'anon can call correct_category';
    exception when insufficient_privilege then null;
    end;
end $$;

-- ── The backend's service role: tokens and apply_predictions (3.4) ──────────

set role service_role;
do $$
declare
    n integer;
begin
    assert (select count(*) from public.gmail_credentials) = 2, 'service_role cannot read gmail_credentials';

    -- A2 takes the prediction; A1 was corrected by the user and must not change
    n := public.apply_predictions('11111111-1111-1111-1111-111111111111', '[
        {"id": "a2000000-0000-0000-0000-000000000002", "category": "Groceries"},
        {"id": "a2000000-0000-0000-0000-000000000001", "category": "Other"}
    ]'::jsonb);
    assert n = 1, format('apply_predictions should update 1 row, updated %s', n);
    assert (select category from public.silver_transactions
             where id = 'a2000000-0000-0000-0000-000000000001') = 'Shopping',
        'a prediction overwrote a user correction';
    assert (select category from public.silver_transactions
             where id = 'a2000000-0000-0000-0000-000000000002') = 'Groceries', 'prediction not written';
    assert (select predicted_at from public.silver_transactions
             where id = 'a2000000-0000-0000-0000-000000000002') is not null, 'predicted_at not set';

    -- A NULL category means "unsure": stored as uncategorised
    n := public.apply_predictions('11111111-1111-1111-1111-111111111111',
        '[{"id": "a2000000-0000-0000-0000-000000000002", "category": null}]'::jsonb);
    assert n = 1, 'apply_predictions should accept a NULL category';
    assert (select not is_categorised from public.silver_transactions
             where id = 'a2000000-0000-0000-0000-000000000002'), 'a NULL category should leave the row uncategorised';

    -- Rows are matched to the given user only
    n := public.apply_predictions('22222222-2222-2222-2222-222222222222',
        '[{"id": "a2000000-0000-0000-0000-000000000002", "category": "Food"}]'::jsonb);
    assert n = 0, 'apply_predictions changed a row of a different user';
end $$;

-- ── Side effects of A's correction, checked as the owner ────────────────────

reset role;
do $$
begin
    assert (select corrected_category from public.category_feedback
             where silver_id = 'a2000000-0000-0000-0000-000000000001') = 'Shopping', 'correction not recorded';
    assert (select original_category from public.category_feedback
             where silver_id = 'a2000000-0000-0000-0000-000000000001') = 'Food', 'original category not recorded';
    assert (select raw_text from public.category_feedback
             where silver_id = 'a2000000-0000-0000-0000-000000000001') = 'VPA zomato@hdfcbank ZOMATO',
        'feedback raw_text should be the bronze receiver';
    assert (select category from public.silver_transactions
             where id = 'b2000000-0000-0000-0000-000000000001') = 'Transport', 'B''s transaction changed';
    assert (select total_amount from public.gold_monthly_summary
             where user_id = '22222222-2222-2222-2222-222222222222') = 250, 'B''s totals changed';
end $$;

-- ── Phase 4: a correction that sticks, and stays where it belongs ──────────
-- 4.1 merchant_key, 4.2 the user's own rule and its spread, 4.4 the shared
-- directory. Seeded as the owner, like the backend's service role.

insert into auth.users (id, email) values
    ('33333333-3333-3333-3333-333333333333', 'c@example.test'),
    ('44444444-4444-4444-4444-444444444444', 'd@example.test'),
    ('55555555-5555-5555-5555-555555555555', 'e@example.test');

-- More of A's Swiggy transactions under one key, one Swiggy transaction of
-- B's, and one unrelated merchant of A's whose name merely starts the same.
insert into public.silver_transactions
    (id, user_id, amount, merchant, merchant_key, transaction_date, source,
     category, is_categorised, user_corrected) values
    ('a3000000-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111',
        60, 'Swiggy', 'upi:swiggy@icici', '2026-09-13', 'gmail', null, false, false),
    ('a3000000-0000-0000-0000-000000000002', '11111111-1111-1111-1111-111111111111',
        70, 'Swiggy', 'upi:swiggy@icici', '2026-09-14', 'gmail', 'Food', true, false),
    ('a3000000-0000-0000-0000-000000000003', '11111111-1111-1111-1111-111111111111',
        80, 'Swiggy', 'upi:swiggy@icici', '2026-09-15', 'gmail', 'Transport', true, true),
    ('a3000000-0000-0000-0000-000000000005', '11111111-1111-1111-1111-111111111111',
        90, 'Swiggy', 'upi:swiggy@icici', '2026-09-16', 'gmail', null, false, false),
    ('a3000000-0000-0000-0000-000000000004', '11111111-1111-1111-1111-111111111111',
        50, 'Sharma Medical', 'name:sharmamedical', '2026-09-16', 'gmail', 'Health', true, false),
    ('b3000000-0000-0000-0000-000000000002', '22222222-2222-2222-2222-222222222222',
        65, 'Swiggy', 'upi:swiggy@icici', '2026-09-14', 'gmail', 'Food', true, false);

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
declare
    r jsonb;
begin
    r := public.correct_category('a3000000-0000-0000-0000-000000000001', 'Shopping');

    -- 4.2: the correction reaches the user's other Swiggy transactions, and
    -- reports how many. Never the row they corrected by hand themselves.
    assert (r ->> 'also_updated')::integer = 2,
        'also_updated should be 2, was ' || coalesce(r ->> 'also_updated', 'null');

    assert (select category from public.silver_transactions
             where id = 'a3000000-0000-0000-0000-000000000002') = 'Shopping',
        'an uncorrected Swiggy row did not follow the correction';
    assert (select category from public.silver_transactions
             where id = 'a3000000-0000-0000-0000-000000000005') = 'Shopping',
        'an uncategorised Swiggy row did not follow the correction';

    -- A row the user corrected by hand is theirs; the rule never overwrites it.
    assert (select category from public.silver_transactions
             where id = 'a3000000-0000-0000-0000-000000000003') = 'Transport',
        'the spread overwrote a row the user had corrected by hand';

    -- 4.3: a merely similar merchant name is untouched.
    assert (select category from public.silver_transactions
             where id = 'a3000000-0000-0000-0000-000000000004') = 'Health',
        'the correction reached an unrelated merchant';

    -- B's Swiggy row is checked further down, as the owner: row-level
    -- security hides it from A here, so reading it would return NULL
    -- whether or not the correction had leaked.

    -- The rule itself
    assert (select category from public.user_merchant_rules
             where user_id = auth.uid() and merchant_key = 'upi:swiggy@icici') = 'Shopping',
        'the correction was not saved as a rule';
    assert (select corrections from public.user_merchant_rules
             where user_id = auth.uid() and merchant_key = 'upi:swiggy@icici') = 1,
        'a first correction should count once';

    -- The feedback row is checked further down, as the owner: the browser is
    -- not allowed to read category_feedback at all.
end $$;

-- Correcting the same merchant a second time is the thing 4.13 measures:
-- it means the rule did not hold, and the count must show it.
do $$
begin
    perform public.correct_category('a3000000-0000-0000-0000-000000000002', 'Food');
    assert (select corrections from public.user_merchant_rules
             where user_id = auth.uid() and merchant_key = 'upi:swiggy@icici') = 2,
        'a repeat correction was not counted';
    assert (select category from public.user_merchant_rules
             where user_id = auth.uid() and merchant_key = 'upi:swiggy@icici') = 'Food',
        'the rule did not take the newer correction';
end $$;

-- A user reads their own rules and nobody else's, and cannot touch the
-- shared directory at all.
do $$
begin
    -- Two rules: Zomato from the correction in 2.1, Swiggy from 4.2. Every
    -- correction becomes a rule, which is what stops it being needed twice.
    assert (select count(*) from public.user_merchant_rules) = 2,
        'A should see exactly their own 2 rules, saw '
        || (select count(*) from public.user_merchant_rules);
    assert (select category from public.user_merchant_rules
             where merchant_key = 'upi:zomato@hdfcbank') = 'Shopping',
        'an ordinary correction did not become a rule';
    assert not exists (select 1 from public.user_merchant_rules where user_id <> auth.uid()),
        'A can see another user''s merchant rules';

    begin
        perform 1 from public.merchant_directory limit 1;
        raise exception 'authenticated can read the merchant directory';
    exception when insufficient_privilege then null;
    end;

    begin
        perform public.refresh_merchant_directory();
        raise exception 'authenticated can rebuild the merchant directory';
    exception when insufficient_privilege then null;
    end;

    begin
        insert into public.user_merchant_rules (user_id, merchant_key, category)
        values (auth.uid(), 'name:hack', 'Food');
        raise exception 'authenticated can write merchant rules directly';
    exception when insufficient_privilege then null;
    end;
end $$;

-- ── 4.4: the shared directory only listens to agreement ────────────────────

reset role;

-- 4.2, checked with full visibility: A's Swiggy correction did not touch B's
-- Swiggy transaction, even though both are the same merchant.
do $$
begin
    assert (select category from public.silver_transactions
             where id = 'b3000000-0000-0000-0000-000000000002') = 'Food',
        'one user''s correction changed another user''s transaction';
    assert not exists (
        select 1 from public.user_merchant_rules
         where user_id = '22222222-2222-2222-2222-222222222222'
    ), 'a correction created a rule for another user';

    -- 4.1: the key is recorded on the feedback row for the weekly retrain.
    assert (select merchant_key from public.category_feedback
             where silver_id = 'a3000000-0000-0000-0000-000000000001') = 'upi:swiggy@icici',
        'the feedback row did not record the merchant key';
end $$;

insert into public.user_merchant_rules (user_id, merchant_key, category, is_person) values
    -- Three unrelated people agree: this becomes shared knowledge.
    ('33333333-3333-3333-3333-333333333333', 'name:netflix', 'Subscription', false),
    ('44444444-4444-4444-4444-444444444444', 'name:netflix', 'Subscription', false),
    ('55555555-5555-5555-5555-555555555555', 'name:netflix', 'Subscription', false),
    -- One person's unusual opinion, and a split one: neither is shared.
    ('33333333-3333-3333-3333-333333333333', 'name:onlyone', 'Groceries', false),
    ('33333333-3333-3333-3333-333333333333', 'name:amazon',  'Shopping',  false),
    ('44444444-4444-4444-4444-444444444444', 'name:amazon',  'Shopping',  false),
    ('55555555-5555-5555-5555-555555555555', 'name:amazon',  'Groceries', false),
    -- A person everyone happens to pay is still a person.
    ('33333333-3333-3333-3333-333333333333', 'upi:rahul@okaxis', 'Transfer', true),
    ('44444444-4444-4444-4444-444444444444', 'upi:rahul@okaxis', 'Transfer', true),
    ('55555555-5555-5555-5555-555555555555', 'upi:rahul@okaxis', 'Transfer', true);

do $$
begin
    perform public.refresh_merchant_directory();

    assert (select category from public.merchant_directory
             where merchant_key = 'name:netflix') = 'Subscription',
        'three users agreeing did not create a directory entry';
    assert (select source from public.merchant_directory
             where merchant_key = 'name:netflix') = 'consensus',
        'the entry should be marked consensus';
    assert (select user_count from public.merchant_directory
             where merchant_key = 'name:netflix') = 3, 'wrong user count';

    -- The requirement: one person's correction never moves anyone else.
    assert not exists (select 1 from public.merchant_directory where merchant_key = 'name:onlyone'),
        'a single user''s correction became shared knowledge';
    assert not exists (select 1 from public.merchant_directory where merchant_key = 'name:amazon'),
        'a disputed merchant became shared knowledge';
    assert not exists (select 1 from public.merchant_directory where merchant_key = 'upi:rahul@okaxis'),
        'a payment to a person became shared knowledge';

    -- A's own correction is not enough to publish Swiggy either.
    assert not exists (select 1 from public.merchant_directory where merchant_key = 'upi:swiggy@icici'),
        'one user''s Swiggy correction became shared knowledge';
end $$;

-- A curated entry outranks consensus and is never overwritten by it.
-- Curating a merchant the consensus already covers is an upsert: the
-- deliberate decision replaces what the votes produced.
insert into public.merchant_directory (merchant_key, category, source)
values ('name:netflix', 'Entertainment', 'curated')
on conflict (merchant_key) do update
   set category   = excluded.category,
       source     = excluded.source,
       updated_at = now();

do $$
begin
    perform public.refresh_merchant_directory();
    assert (select category from public.merchant_directory
             where merchant_key = 'name:netflix') = 'Entertainment',
        'consensus overwrote a curated entry';
end $$;

-- ── 4.1: the SQL key agrees with backend/merchant_identity.py ──────────────
-- These are MERCHANT_KEY_CASES in backend/test_merchant_identity.py.

do $$
begin
    assert public.merchant_key_of('VPA swiggy@icici SWIGGY', 'Swiggy') = 'upi:swiggy@icici',
        'merchant_key_of disagrees on a UPI payee id';
    assert public.merchant_key_of('VPA swiggy@icici SWIGGY ORDER 4412', 'Swiggy') = 'upi:swiggy@icici',
        'merchant_key_of disagrees when trailing text differs';
    assert public.merchant_key_of('VPA paytmqr2njw85@ptys NBC Vikhroli', 'NBC Vikhroli') = 'upi:paytmqr2njw85@ptys',
        'merchant_key_of disagrees on a per-shop QR code';
    assert public.merchant_key_of('VPA paytm.k42v9be@pty NBC Vikhroli W', 'NBC Vikhroli W') = 'upi:paytm.k42v9be@pty',
        'merchant_key_of disagrees on a dotted payee id';
    assert public.merchant_key_of('VPA gpay-70418293355@okbizaxis Food Xpress', 'Food Xpress') = 'upi:gpay-70418293355@okbizaxis',
        'merchant_key_of disagrees on a hyphenated payee id';
    assert public.merchant_key_of('VPA SWIGGY@ICICI Swiggy', 'Swiggy') = 'upi:swiggy@icici',
        'merchant_key_of is case sensitive';
    assert public.merchant_key_of('NETFLIX INDIA', 'Netflix') = 'name:netflix',
        'merchant_key_of disagrees on a plain name';
    assert public.merchant_key_of('NBC Vikhroli W', 'NBC Vikhroli W') = 'name:nbcvikhroliw',
        'merchant_key_of disagrees on a spaced name';
    assert public.merchant_key_of('nbc vikhroli-w', 'NBC Vikhroli W') = 'name:nbcvikhroliw',
        'merchant_key_of disagrees on punctuation';
    assert public.merchant_key_of('Sharma General Store', 'Sharma General Store') = 'name:sharmageneralstore',
        'merchant_key_of disagrees on a shop name';
    assert public.merchant_key_of('Sharma Medical', 'Sharma Medical') = 'name:sharmamedical',
        'merchant_key_of disagrees on a second shop name';
    assert public.merchant_key_of('Sharma General Store', 'Sharma General Store')
        <> public.merchant_key_of('Sharma Medical', 'Sharma Medical'),
        'two different shops share one key';
    assert public.merchant_key_of('', '') is null, 'an empty receiver should have no key';
    assert public.merchant_key_of('!!!', '') is null, 'punctuation alone should have no key';
    assert public.merchant_key_of('paid 60@2 to shop', 'Shop') = 'name:shop',
        'an amount was mistaken for a UPI payee id';
end $$;

-- ── 4.10 and 4.13: the model registry and the quality report ───────────────
-- Both are backend-only. A signed-in user must not be able to see which model
-- is live, promote one, or read anyone's aggregate numbers.

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
begin
    begin
        perform 1 from public.model_registry limit 1;
        raise exception 'authenticated can read the model registry';
    exception when insufficient_privilege then null;
    end;

    begin
        perform public.activate_model('anything');
        raise exception 'authenticated can activate a model';
    exception when insufficient_privilege then null;
    end;

    begin
        perform public.quality_report();
        raise exception 'authenticated can read the quality report';
    exception when insufficient_privilege then null;
    end;
end $$;

reset role;

insert into public.model_registry (version, storage_path, sha256, status, trained_at) values
    ('v1', 'models/v1.pkl', 'aaa111', 'candidate', now()),
    ('v2', 'models/v2.pkl', 'bbb222', 'candidate', now());

do $$
declare
    r jsonb;
begin
    r := public.activate_model('v1');
    assert (r ->> 'ok')::boolean, 'activate_model did not return ok';
    assert r ->> 'previous' is null, 'there should have been no previous active model';
    assert (select status from public.model_registry where version = 'v1') = 'active',
        'v1 was not activated';
    assert (select activated_at from public.model_registry where version = 'v1') is not null,
        'activated_at was not set';

    -- Promoting the next one retires the old one in the same transaction.
    r := public.activate_model('v2');
    assert r ->> 'previous' = 'v1', 'the previous version was not reported';
    assert (select status from public.model_registry where version = 'v1') = 'rolled_back',
        'the old model was not rolled back';
    assert (select status from public.model_registry where version = 'v2') = 'active',
        'v2 was not activated';
    assert (select count(*) from public.model_registry where status = 'active') = 1,
        'there must be exactly one active model';

    -- The database, not the code, is what guarantees a single live model.
    begin
        update public.model_registry set status = 'active' where version = 'v1';
        raise exception 'two models were allowed to be active at once';
    exception when unique_violation then null;
    end;

    begin
        perform public.activate_model('does-not-exist');
        raise exception 'an unknown model version was activated';
    exception when sqlstate 'P0002' then null;
    end;
end $$;

-- 4.13: the report, and the number that matters most in it.
do $$
declare
    q jsonb;
begin
    q := public.quality_report(now() - interval '1 day');

    assert q ? 'repeat_corrections',  'the report has no repeat_corrections';
    assert q ? 'uncategorised_share', 'the report has no uncategorised_share';
    assert q ? 'corrections',         'the report has no corrections count';

    -- A corrected Swiggy twice earlier in this file, which is exactly the
    -- thing this metric exists to surface: a rule that did not hold.
    assert (q ->> 'repeat_corrections')::integer = 1,
        'repeat_corrections should be 1, was ' || coalesce(q ->> 'repeat_corrections', 'null');
    assert (q ->> 'extra_corrections')::integer = 1,
        'extra_corrections should be 1, was ' || coalesce(q ->> 'extra_corrections', 'null');
    assert q ->> 'active_model' = 'v2', 'the report should name the active model';
    assert (q ->> 'user_rules')::integer >= 2, 'the report should count the rules';

    -- With no transactions in the window the share is NULL, never a made-up 0.
    q := public.quality_report(now() + interval '1 day');
    assert q ->> 'uncategorised_share' is null,
        'an empty window should report NULL, not a fabricated 0';
end $$;

-- ── WP4: sync counters and one active sync per user ─────────────────────────

do $$
declare
    a constant uuid := '11111111-1111-1111-1111-111111111111';
    b constant uuid := '22222222-2222-2222-2222-222222222222';
begin
    -- B already has a 'running' job from the seed: a second active one is refused.
    begin
        insert into public.sync_jobs (user_id, kind, status) values (b, 'manual', 'queued');
        raise exception 'two active sync jobs were allowed for one user';
    exception when unique_violation then null;
    end;

    -- A has only a finished job, so one active job is fine, a second is not,
    -- and finished jobs never count.
    insert into public.sync_jobs (user_id, kind, status) values (a, 'manual', 'queued');
    begin
        insert into public.sync_jobs (user_id, kind, status) values (a, 'scheduled', 'running');
        raise exception 'a second active job was allowed for A';
    exception when unique_violation then null;
    end;
    update public.sync_jobs set status = 'succeeded', finished_at = now()
     where user_id = a and status = 'queued';
    insert into public.sync_jobs (user_id, kind, status) values (a, 'manual', 'succeeded');
    insert into public.sync_jobs (user_id, kind, status) values (a, 'manual', 'failed');

    -- Counters start NULL (not recorded), never 0.
    assert not exists (select 1 from public.sync_jobs
                        where messages_listed is not null or messages_failed is not null
                           or alerts_parsed is not null),
        'counters must default to NULL';
    update public.sync_jobs set messages_listed = 12, messages_failed = 1, alerts_parsed = 9
     where id = (select id from public.sync_jobs where user_id = a order by created_at desc, id limit 1);
end $$;

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
begin
    assert exists (select 1 from public.sync_jobs where alerts_parsed = 9),
        'A should read the counters of their own jobs';
    assert not exists (select 1 from public.sync_jobs where user_id <> auth.uid()),
        'A can see another user''s sync jobs';
    begin
        update public.sync_jobs set alerts_parsed = 99;
        raise exception 'authenticated can write sync counters';
    exception when insufficient_privilege then null;
    end;
end $$;

reset role;

-- ── WP6: event log ──────────────────────────────────────────────────────────

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
begin
    perform public.log_event('dashboard_viewed', '{"months_available": 3, "has_unsure": true}');
    perform public.log_event('review_opened');   -- props default to {}

    begin
        perform public.log_event('signup_completed');
        raise exception 'an event name outside the allow-list was accepted';
    exception when sqlstate '22023' then null;
    end;
    begin
        perform public.log_event('dashboard_viewed', '[1,2]');
        raise exception 'props that are not an object were accepted';
    exception when sqlstate '22023' then null;
    end;
    begin
        perform public.log_event('dashboard_viewed', jsonb_build_object('x', repeat('y', 2000)));
        raise exception 'oversized props were accepted';
    exception when sqlstate '22023' then null;
    end;

    -- The table itself is closed to the browser.
    begin
        perform 1 from public.app_events limit 1;
        raise exception 'authenticated can read app_events';
    exception when insufficient_privilege then null;
    end;
    begin
        insert into public.app_events (user_id, name) values (auth.uid(), 'dashboard_viewed');
        raise exception 'authenticated can insert into app_events directly';
    exception when insufficient_privilege then null;
    end;
end $$;

-- Signed out, and anon, cannot log.
select set_config('request.jwt.claims', '{"role":"authenticated"}', false);
do $$
begin
    begin
        perform public.log_event('dashboard_viewed');
        raise exception 'log_event worked without a user';
    exception when sqlstate '28000' then null;
    end;
end $$;

set role anon;
select set_config('request.jwt.claims', '{"role":"anon"}', false);
do $$
begin
    begin
        perform public.log_event('dashboard_viewed');
        raise exception 'anon can call log_event';
    exception when insufficient_privilege then null;
    end;
end $$;

reset role;

do $$
declare
    a constant uuid := '11111111-1111-1111-1111-111111111111';
begin
    assert (select count(*) from public.app_events where user_id = a) = 2,
        'exactly the two valid events should be stored';

    -- The hourly cap: after 200 events in an hour more are dropped, without an error.
    insert into public.app_events (user_id, name)
    select a, 'month_changed' from generate_series(1, 198);
    assert (select count(*) from public.app_events where user_id = a) = 200, 'seed should reach the cap';
end $$;

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);
select public.log_event('month_changed');
reset role;

do $$
begin
    assert (select count(*) from public.app_events
             where user_id = '11111111-1111-1111-1111-111111111111') = 200,
        'an event over the hourly cap should be dropped';
end $$;

-- ── WP9: backfill jobs and history_from ─────────────────────────────────────

do $$
declare
    a constant uuid := '11111111-1111-1111-1111-111111111111';
begin
    insert into public.sync_jobs (user_id, kind, status) values (a, 'backfill', 'succeeded');
    begin
        insert into public.sync_jobs (user_id, kind, status) values (a, 'something-else', 'succeeded');
        raise exception 'an unknown sync kind was accepted';
    exception when check_violation then null;
    end;

    assert (select history_from from public.gmail_sync where user_id = a) is null,
        'history_from should start as NULL (not recorded)';
    update public.gmail_sync set history_from = '2026-08-31T18:30:00Z' where user_id = a;
end $$;

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
begin
    assert (select history_from from public.gmail_sync) = '2026-08-31T18:30:00Z',
        'A should read history_from of their own connection';
    begin
        update public.gmail_sync set history_from = null;
        raise exception 'authenticated can write history_from';
    exception when insufficient_privilege then null;
    end;
end $$;

reset role;

-- ── WP13a: a user can remove their own merchant rule ────────────────────────

insert into public.user_merchant_rules (user_id, merchant_key, category) values
    ('11111111-1111-1111-1111-111111111111', 'name:rule-removal-test', 'Food'),
    ('22222222-2222-2222-2222-222222222222', 'name:rule-removal-test', 'Transport');

set role authenticated;
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated"}', false);

do $$
declare
    r jsonb;
    silver_before integer;
begin
    select count(*) into silver_before from public.silver_transactions;

    r := public.delete_merchant_rule('name:rule-removal-test');
    assert (r ->> 'deleted')::integer = 1, 'the rule should be deleted, got ' || r::text;
    assert not exists (select 1 from public.user_merchant_rules where merchant_key = 'name:rule-removal-test'),
        'A still sees the removed rule';

    r := public.delete_merchant_rule('name:rule-removal-test');
    assert (r ->> 'deleted')::integer = 0, 'removing it again should delete nothing';

    r := public.delete_merchant_rule('name:does-not-exist');
    assert (r ->> 'deleted')::integer = 0, 'an unknown key should delete nothing';

    assert (select count(*) from public.silver_transactions) = silver_before,
        'removing a rule must not change any payment';

    begin
        perform public.delete_merchant_rule('');
        raise exception 'an empty key was accepted';
    exception when sqlstate '22023' then null;
    end;

    begin
        delete from public.user_merchant_rules;
        raise exception 'authenticated can delete rules directly';
    exception when insufficient_privilege then null;
    end;
end $$;

set role anon;
select set_config('request.jwt.claims', '{"role":"anon"}', false);
do $$
begin
    begin
        perform public.delete_merchant_rule('name:rule-removal-test');
        raise exception 'anon can call delete_merchant_rule';
    exception when insufficient_privilege then null;
    end;
end $$;

reset role;

do $$
begin
    -- B's rule with the same key is untouched by A's removal.
    assert (select category from public.user_merchant_rules
             where user_id = '22222222-2222-2222-2222-222222222222'
               and merchant_key = 'name:rule-removal-test') = 'Transport',
        'A removed another user''s rule';
end $$;

-- ── 2.8: deleting an account removes every row it owns, and only those ─────

delete from auth.users where id = '11111111-1111-1111-1111-111111111111';

do $$
declare
    t text;
    n integer;
begin
    foreach t in array array['transactions', 'bronze_transactions', 'silver_transactions',
                             'gold_monthly_summary', 'gmail_sync', 'category_feedback',
                             'gmail_credentials', 'oauth_states', 'sync_jobs',
                             'user_merchant_rules', 'app_events'] loop
        execute format('select count(*) from public.%I where user_id = %L',
                       t, '11111111-1111-1111-1111-111111111111') into n;
        assert n = 0, format('%s still has %s rows for the deleted user', t, n);
    end loop;

    -- B keeps both rows: the Uber one from the first seed and the Swiggy one
    -- added for the Phase 4 checks.
    assert (select count(*) from public.silver_transactions
             where user_id = '22222222-2222-2222-2222-222222222222') = 2,
        'B''s data was deleted too, B has '
        || (select count(*) from public.silver_transactions
             where user_id = '22222222-2222-2222-2222-222222222222') || ' rows';
end $$;

\echo 'ALL SECURITY CHECKS PASSED'
