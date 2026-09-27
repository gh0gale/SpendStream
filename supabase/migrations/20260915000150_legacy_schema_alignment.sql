-- Legacy schema alignment (added 2026-09-26).
--
-- Projects created before 2026-09-15 skip the baseline migration because
-- their tables already exist, but those tables were created by hand and
-- differ from the baseline. Found on the real dev project when the Phase 3
-- migration failed with: column "message_id" named in key does not exist.
--
-- Brings a legacy project up to what Phases 3 and 4 and the backend expect.
-- Every step is guarded, so on a project built from the baseline this file
-- changes nothing.

-- Raw rows carry their Gmail message id (Phase 3 makes it unique per user).
alter table public.transactions add column if not exists message_id text;

-- correct_category() upserts feedback on (user_id, silver_id). Legacy tables
-- have a surrogate id instead, so repeated corrections of one row could
-- exist. Keep the newest per row, then add the key.
do $$
begin
    if not exists (
        select 1
          from pg_index i
          join pg_class c on c.oid = i.indrelid
          join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'public'
           and c.relname = 'category_feedback'
           and i.indisunique
           and (select array_agg(a.attname::text order by a.attname)
                  from unnest(i.indkey) k
                  join pg_attribute a on a.attrelid = c.oid and a.attnum = k)
               = array['silver_id', 'user_id']
    ) then
        delete from public.category_feedback f
         using (
             select ctid,
                    row_number() over (
                        partition by user_id, silver_id
                        order by corrected_at desc nulls last
                    ) as rn
               from public.category_feedback
         ) ranked
         where f.ctid = ranked.ctid
           and ranked.rn > 1;

        alter table public.category_feedback
            add constraint category_feedback_user_silver_key unique (user_id, silver_id);
    end if;
end;
$$;

-- The raw -> bronze stage upserts on fingerprint. Duplicates cannot be
-- removed safely here (silver rows point at bronze rows), so a legacy table
-- with duplicate fingerprints stops the migration with a clear message.
do $$
begin
    if not exists (
        select 1
          from pg_index i
          join pg_class c on c.oid = i.indrelid
          join pg_namespace n on n.oid = c.relnamespace
          join pg_attribute a on a.attrelid = c.oid and a.attnum = i.indkey[0]
         where n.nspname = 'public'
           and c.relname = 'bronze_transactions'
           and i.indisunique
           and i.indnatts = 1
           and a.attname = 'fingerprint'
    ) then
        if exists (
            select 1 from public.bronze_transactions
             where fingerprint is not null
             group by fingerprint having count(*) > 1
        ) then
            raise exception 'bronze_transactions has duplicate fingerprints; resolve them before applying this migration';
        end if;

        alter table public.bronze_transactions
            add constraint bronze_transactions_fingerprint_key unique (fingerprint);
    end if;
end;
$$;
