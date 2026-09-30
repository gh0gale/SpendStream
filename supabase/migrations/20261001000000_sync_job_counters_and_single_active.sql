-- Scalability plan WP4 (docs/scalability/04-implementation-plan.md).
--
-- 1. What each sync saw, so silent parse failures become numbers:
--    messages_listed  Gmail messages the search matched (new mail plus retried ids)
--    messages_failed  of those, how many could not be downloaded (retried next sync)
--    alerts_parsed    of those, how many parsed as a debit alert
--    All NULL on rows written before this migration and on syncs that failed
--    before reading mail: NULL means "not recorded", never 0.
--    Written only by the backend (service role). The browser keeps its
--    existing select-only grant on sync_jobs, so it can read these columns.
--
-- 2. One active sync per user, enforced by the database. A second manual or
--    scheduled sync while one is queued or running would repeat the same Gmail
--    reads; the backend refuses it, and this index is the backstop for races.

alter table public.sync_jobs
    add column if not exists messages_listed integer,
    add column if not exists messages_failed integer,
    add column if not exists alerts_parsed   integer;

comment on column public.sync_jobs.messages_listed is 'Gmail messages this sync tried to read; NULL = not recorded.';
comment on column public.sync_jobs.messages_failed is 'Of messages_listed, how many could not be downloaded.';
comment on column public.sync_jobs.alerts_parsed   is 'Of messages_listed, how many parsed as a debit alert.';

-- The index cannot be built while a user has several active rows. A row that
-- has been queued or running for over 30 minutes died with its process (the
-- backend's own cutoff); of the rest keep only each user's newest.
update public.sync_jobs
   set status      = 'failed',
       error       = 'Interrupted by a server restart. Sync again.',
       finished_at = now()
 where status in ('queued', 'running')
   and created_at < now() - interval '30 minutes';

update public.sync_jobs j
   set status      = 'failed',
       error       = 'Interrupted by a server restart. Sync again.',
       finished_at = now()
  from (
        select id,
               row_number() over (partition by user_id order by created_at desc, id) as rn
          from public.sync_jobs
         where status in ('queued', 'running')
       ) ranked
 where j.id = ranked.id
   and ranked.rn > 1;

create unique index if not exists sync_jobs_one_active
    on public.sync_jobs (user_id)
 where status in ('queued', 'running');
