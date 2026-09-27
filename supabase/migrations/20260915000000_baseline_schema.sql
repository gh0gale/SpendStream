-- Baseline schema: the tables the code used before any migration existed.
--
-- Reconstructed from code (docs/MASTER_DOCUMENTATION.md Appendix B) and made
-- stricter in one way: every user_id references auth.users with
-- ON DELETE CASCADE, and the pipeline's own foreign keys cascade too, so
-- deleting an account deletes all of its rows.
--
-- Written for a new Supabase project. On a project that already has these
-- tables, this file fails on the first CREATE TABLE; add the foreign keys by
-- hand there instead of editing this file.

-- Raw transactions (everything ingested from Gmail)
create table public.transactions (
    id                uuid        primary key default gen_random_uuid(),
    user_id           uuid        not null references auth.users(id) on delete cascade,
    amount            numeric     not null,
    receiver          text,
    transaction_type  text        default 'debit',
    "timestamp"       timestamptz,
    source            text,                    -- 'gmail' ('file' on rows from before 2026-09-15)
    raw_text          text,
    message_id        text                     -- Gmail message id
);
create index transactions_user_id_idx on public.transactions(user_id);

-- Bronze (deduplicated)
create table public.bronze_transactions (
    id                uuid        primary key default gen_random_uuid(),
    raw_id            uuid        references public.transactions(id) on delete cascade,
    user_id           uuid        not null references auth.users(id) on delete cascade,
    amount            numeric,
    receiver          text,
    transaction_type  text,
    "timestamp"       timestamptz,
    source            text,
    raw_text          text,
    fingerprint       text        unique,      -- SHA-256 of user_id|amount|receiver|YYYY-MM-DD
    message_id        text,
    is_duplicate      boolean     default false,
    created_at        timestamptz default now()
);
create index bronze_transactions_user_id_idx on public.bronze_transactions(user_id);
create index bronze_transactions_user_dup_idx on public.bronze_transactions(user_id, is_duplicate);

-- Silver (cleaned and categorised)
create table public.silver_transactions (
    id                uuid        primary key default gen_random_uuid(),
    bronze_id         uuid        references public.bronze_transactions(id) on delete cascade,
    user_id           uuid        not null references auth.users(id) on delete cascade,
    amount            numeric,
    merchant          text,
    transaction_type  text,
    transaction_date  date,
    source            text,
    category          text,                    -- NULL while uncategorised
    is_categorised    boolean     default false,
    user_corrected    boolean     default false,
    created_at        timestamptz default now()
);
create index silver_transactions_user_id_idx on public.silver_transactions(user_id);
create index silver_transactions_user_cat_idx on public.silver_transactions(user_id, is_categorised);

-- Gold (monthly totals per category)
create table public.gold_monthly_summary (
    user_id       uuid        not null references auth.users(id) on delete cascade,
    month         date        not null,        -- always the first of the month
    category      text        not null,
    total_amount  numeric,
    txn_count     integer,
    updated_at    timestamptz,
    primary key (user_id, month, category)
);

-- Gmail connection (tokens move out of this table in the next migration)
create table public.gmail_sync (
    user_id       uuid        primary key references auth.users(id) on delete cascade,
    access_token  text,
    refresh_token text,
    last_fetched  timestamptz,                 -- incremental fetch cursor
    updated_at    timestamptz
);

-- User corrections
create table public.category_feedback (
    user_id             uuid        not null references auth.users(id) on delete cascade,
    silver_id           uuid        not null,
    merchant            text,
    raw_text            text,
    original_category   text,
    corrected_category  text,
    amount              numeric,
    corrected_at        timestamptz,
    primary key (user_id, silver_id)
);
create index category_feedback_user_time_idx on public.category_feedback(user_id, corrected_at desc);
