-- Data as it could exist before the Phase 3 migration, for phase3_backfill_checks.sql.
-- run_local.sh applies: stub, baseline, Phase 2, this file, Phase 3, the checks.
-- User C: 33333333-3333-3333-3333-333333333333

insert into auth.users (id, email) values
    ('33333333-3333-3333-3333-333333333333', 'c@example.test');

insert into public.transactions (id, user_id, amount, receiver, "timestamp", source) values
    ('c0000000-0000-0000-0000-000000000001', '33333333-3333-3333-3333-333333333333', 100, 'VPA a@okaxis A', '2026-09-01T10:00:00Z', 'gmail'),  -- already in bronze
    ('c0000000-0000-0000-0000-000000000002', '33333333-3333-3333-3333-333333333333', 200, 'VPA b@okaxis B', '2026-09-02T10:00:00Z', 'gmail');  -- never processed

insert into public.bronze_transactions (id, raw_id, user_id, amount, receiver, "timestamp", source, fingerprint) values
    ('c1000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', '33333333-3333-3333-3333-333333333333', 100, 'VPA a@okaxis A', '2026-09-01T10:00:00Z', 'gmail', 'fp-c1'),  -- in silver
    ('c1000000-0000-0000-0000-000000000002', null,                                   '33333333-3333-3333-3333-333333333333', 300, 'VPA d@okaxis D', '2026-09-03T10:00:00Z', 'gmail', 'fp-c2');  -- not yet in silver

insert into public.silver_transactions
       (id, bronze_id, user_id, amount, merchant, transaction_date, source, category, is_categorised, user_corrected, created_at) values
    -- Two silver rows for one bronze row: an older uncorrected one and a corrected one
    ('c2000000-0000-0000-0000-000000000001', 'c1000000-0000-0000-0000-000000000001', '33333333-3333-3333-3333-333333333333', 100, 'A', '2026-09-01', 'gmail', 'Food',     true,  false, '2026-09-01T11:00:00Z'),
    ('c2000000-0000-0000-0000-000000000002', 'c1000000-0000-0000-0000-000000000001', '33333333-3333-3333-3333-333333333333', 100, 'A', '2026-09-01', 'gmail', 'Shopping', true,  true,  '2026-09-01T12:00:00Z'),
    -- Uncategorised, never predicted
    ('c2000000-0000-0000-0000-000000000003', null,                                   '33333333-3333-3333-3333-333333333333',  50, 'E', '2026-09-04', 'gmail', null,       false, false, '2026-09-04T11:00:00Z');
