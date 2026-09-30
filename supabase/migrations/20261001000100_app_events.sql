-- Scalability plan WP6: a first-party event log, so activation and retention
-- can be measured without a third-party script (the site's CSP allows none).
--
-- Only signed-in users can log, only through log_event(), which checks the
-- event name against an allow-list, the size of props, and a per-user hourly
-- cap. props must never hold merchant names, amounts or email text; the
-- frontend (src/lib/track.js) sends counts and short labels only.
-- Nothing in the browser can read or write the table directly. The backend
-- deletes rows older than 90 days (tasks.prune_old_rows).
-- Sign-up and account deletion are not events: they happen before a session
-- exists or remove the user's rows, and auth.users already records sign-ups.

create table public.app_events (
    id          bigint      generated always as identity primary key,
    user_id     uuid        not null references auth.users(id) on delete cascade,
    name        text        not null,
    props       jsonb       not null default '{}'::jsonb,
    created_at  timestamptz not null default now()
);

create index app_events_user_created_idx on public.app_events (user_id, created_at desc);
create index app_events_created_idx      on public.app_events (created_at);

alter table public.app_events enable row level security;
revoke all on public.app_events from anon, authenticated;

create function public.log_event(p_name text, p_props jsonb default '{}'::jsonb)
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_user_id uuid := auth.uid();
begin
    if v_user_id is null then
        raise exception 'Not signed in' using errcode = '28000';
    end if;

    -- Must match the events fired by frontend/src (docs/scalability/02-product.md section 8).
    if p_name is null or p_name not in (
        'connect_step_viewed', 'google_consent_started', 'gmail_result', 'sync_finished',
        'dashboard_viewed', 'month_changed', 'review_opened', 'review_cleared',
        'category_corrected', 'filter_used', 'reconnect_prompt_shown'
    ) then
        raise exception 'Unknown event: %', p_name using errcode = '22023';
    end if;

    if p_props is null or jsonb_typeof(p_props) <> 'object' then
        raise exception 'props must be a JSON object' using errcode = '22023';
    end if;
    if pg_column_size(p_props) > 1024 then
        raise exception 'props too large' using errcode = '22023';
    end if;

    -- Over the cap the event is dropped quietly: logging must never break the app.
    if (select count(*) from public.app_events
         where user_id = v_user_id and created_at > now() - interval '1 hour') >= 200 then
        return;
    end if;

    insert into public.app_events (user_id, name, props)
    values (v_user_id, p_name, p_props);
end;
$$;

revoke execute on function public.log_event(text, jsonb) from public, anon;
grant execute on function public.log_event(text, jsonb) to authenticated;
