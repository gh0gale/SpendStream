-- After every migration except the baseline has run on the legacy tables
-- (legacy_schema_seed.sql), the objects the backend relies on must exist.

do $$
declare
    n integer;
begin
    if not exists (select 1 from information_schema.columns
                    where table_schema = 'public' and table_name = 'transactions'
                      and column_name = 'message_id') then
        raise exception 'legacy: transactions.message_id missing';
    end if;

    select count(*) into n from public.category_feedback
     where silver_id = 'd2000000-0000-0000-0000-000000000001';
    if n <> 1 then
        raise exception 'legacy: expected 1 feedback row after dedup, found %', n;
    end if;
    if (select corrected_category from public.category_feedback
         where silver_id = 'd2000000-0000-0000-0000-000000000001') <> 'Food' then
        raise exception 'legacy: dedup kept the older correction';
    end if;

    -- The feedback key works as correct_category() uses it.
    insert into public.category_feedback (user_id, silver_id, corrected_category, corrected_at)
    values ('44444444-4444-4444-4444-444444444444', 'd2000000-0000-0000-0000-000000000001', 'Shopping', now())
    on conflict (user_id, silver_id) do update set corrected_category = excluded.corrected_category;

    -- The fingerprint key works as the raw -> bronze stage uses it.
    insert into public.bronze_transactions (user_id, amount, fingerprint)
    values ('44444444-4444-4444-4444-444444444444', 1, 'fp-d1')
    on conflict (fingerprint) do nothing;
    select count(*) into n from public.bronze_transactions where fingerprint = 'fp-d1';
    if n <> 1 then
        raise exception 'legacy: fingerprint is not unique';
    end if;

    -- Later phases ran: message-id key, work-queue markers, merchant keys, view.
    insert into public.transactions (user_id, amount, message_id)
    values ('44444444-4444-4444-4444-444444444444', 1, 'm-1');
    begin
        insert into public.transactions (user_id, amount, message_id)
        values ('44444444-4444-4444-4444-444444444444', 1, 'm-1');
        raise exception 'legacy: duplicate message id accepted';
    exception when unique_violation then null;
    end;

    if (select merchant_key from public.silver_transactions
         where id = 'd2000000-0000-0000-0000-000000000001') is null then
        raise exception 'legacy: Phase 4 did not backfill merchant_key';
    end if;
    if (select relkind from pg_class where relname = 'gold_monthly_summary') <> 'v' then
        raise exception 'legacy: gold_monthly_summary is not a view';
    end if;

    raise notice 'legacy schema checks passed';
end;
$$;
