-- DATA-07, revised 2026-09-27: the Gmail cursor (gmail_sync.last_fetched)
-- now always moves to the start of a sync. Messages Gmail would not hand over
-- during that sync (rate limits, server errors) are kept here, and the next
-- sync retries them along with the new mail. Before this, a single failed
-- message held the cursor back, so every sync re-read the whole first window (90 days then) and the burst
-- itself tripped Gmail's rate limit.
--
-- Written only by the backend (service role). The browser keeps its existing
-- select-only access to its own gmail_sync row; it never needs this column.

alter table public.gmail_sync
    add column if not exists pending_message_ids text[] not null default '{}';

comment on column public.gmail_sync.pending_message_ids is
    'Gmail message ids the last sync could not download; the next sync retries them (DATA-07).';
