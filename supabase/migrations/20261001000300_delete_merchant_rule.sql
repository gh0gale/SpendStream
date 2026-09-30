-- Scalability plan WP13a: a user can see their merchant rules and remove one.
--
-- Removing a rule only deletes the user's row in user_merchant_rules. Payments
-- already sorted keep their category (corrected rows stay corrected), so
-- nothing the user sees changes; new payments to that merchant go back to the
-- shared directory and the model. The browser still has no write grant on the
-- table: this function is the only way, scoped to auth.uid().

create function public.delete_merchant_rule(p_merchant_key text)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
    v_user_id uuid := auth.uid();
    v_deleted integer;
begin
    if v_user_id is null then
        raise exception 'Not signed in' using errcode = '28000';
    end if;
    if p_merchant_key is null or p_merchant_key = '' then
        raise exception 'merchant key required' using errcode = '22023';
    end if;

    delete from public.user_merchant_rules
     where user_id = v_user_id
       and merchant_key = p_merchant_key;
    get diagnostics v_deleted = row_count;

    return jsonb_build_object('ok', true, 'deleted', v_deleted);
end;
$$;

revoke execute on function public.delete_merchant_rule(text) from public, anon;
grant execute on function public.delete_merchant_rule(text) to authenticated;
