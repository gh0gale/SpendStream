-- Scalability plan WP9: "read an earlier month".
--
-- gmail_sync.history_from: the start (India midnight) of the oldest calendar
-- month of bank alerts read so far. NULL = not recorded (users from before
-- this migration); the backend then derives it from the earliest stored
-- payment. Written only by the backend (service role); the browser reads it
-- through its existing select-only grant, to name the next month on the button.
--
-- sync_jobs.kind gains 'backfill': a job that reads one earlier month and
-- never moves the normal read cursor.

alter table public.gmail_sync
    add column if not exists history_from timestamptz;

comment on column public.gmail_sync.history_from is
    'Start of the oldest month of bank alerts read so far (India midnight); NULL = not recorded.';

alter table public.sync_jobs drop constraint if exists sync_jobs_kind_check;
alter table public.sync_jobs
    add constraint sync_jobs_kind_check check (kind in ('manual', 'scheduled', 'backfill'));
