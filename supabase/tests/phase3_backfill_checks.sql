-- Checks that the Phase 3 migration backfilled existing data correctly.
-- Seeded by phase3_backfill_seed.sql; run by run_local.sh in its own database.

\set ON_ERROR_STOP on

do $$
begin
    assert (select processed_at from public.transactions
             where id = 'c0000000-0000-0000-0000-000000000001') is not null,
        'a raw row already in bronze should be marked processed';
    assert (select processed_at from public.transactions
             where id = 'c0000000-0000-0000-0000-000000000002') is null,
        'a raw row not yet in bronze should stay pending';

    assert (select processed_at from public.bronze_transactions
             where id = 'c1000000-0000-0000-0000-000000000001') is not null,
        'a bronze row already in silver should be marked processed';
    assert (select processed_at from public.bronze_transactions
             where id = 'c1000000-0000-0000-0000-000000000002') is null,
        'a bronze row not yet in silver should stay pending';

    assert not exists (select 1 from public.silver_transactions
                        where id = 'c2000000-0000-0000-0000-000000000001'),
        'the uncorrected duplicate silver row should be removed';
    assert exists (select 1 from public.silver_transactions
                    where id = 'c2000000-0000-0000-0000-000000000002'),
        'the user-corrected duplicate should be kept';

    assert (select predicted_at from public.silver_transactions
             where id = 'c2000000-0000-0000-0000-000000000002') is not null,
        'categorised rows should count as predicted';
    assert (select predicted_at from public.silver_transactions
             where id = 'c2000000-0000-0000-0000-000000000003') is null,
        'uncategorised rows should stay pending for prediction';

    assert (select total_amount from public.gold_monthly_summary
             where user_id = '33333333-3333-3333-3333-333333333333' and category = 'Shopping') = 100,
        'the totals view should count the kept row';
    assert not exists (select 1 from public.gold_monthly_summary
                        where user_id = '33333333-3333-3333-3333-333333333333' and category = 'Food'),
        'the removed duplicate should not count';
end $$;

\echo 'PHASE 3 BACKFILL CHECKS PASSED'
