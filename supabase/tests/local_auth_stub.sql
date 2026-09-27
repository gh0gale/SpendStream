-- Local stand-in for the parts of Supabase the migrations depend on.
-- FOR LOCAL TESTS ONLY: never apply this to a Supabase project, which already
-- has these roles, the auth schema and auth.uid().
--
-- Mirrors Supabase in the ways the security tests rely on:
--   * roles anon, authenticated, service_role (service_role bypasses RLS);
--   * auth.users and auth.uid(), which reads the JWT "sub" claim;
--   * default privileges that grant every new public table and function to
--     anon, authenticated and service_role, so the tests prove the migrations
--     revoke what they must.
-- Roles are cluster-wide, so creating them tolerates a second database.

do $$ begin create role anon          nologin;           exception when duplicate_object then null; end $$;
do $$ begin create role authenticated nologin;           exception when duplicate_object then null; end $$;
do $$ begin create role service_role  nologin bypassrls; exception when duplicate_object then null; end $$;

create schema auth;
grant usage on schema auth to anon, authenticated, service_role;

create table auth.users (
    id    uuid primary key,
    email text
);

-- Same definition Supabase uses.
create function auth.uid() returns uuid
language sql stable
as $$
    select coalesce(
        nullif(current_setting('request.jwt.claim.sub', true), ''),
        (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
    )::uuid
$$;
grant execute on function auth.uid() to anon, authenticated, service_role;

grant usage on schema public to anon, authenticated, service_role;
alter default privileges in schema public grant all on tables    to anon, authenticated, service_role;
alter default privileges in schema public grant all on functions to anon, authenticated, service_role;
