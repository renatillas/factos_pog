-- migrate:up
alter table factos_outbox
  add column lock_token text,
  add column failed_at timestamptz;

-- Leases issued by the pre-token implementation cannot be finalized safely after
-- this migration, so release them for leasing under the guarded protocol.
update factos_outbox
set locked_until = null
where status = 'pending';

alter table factos_outbox
  add constraint factos_outbox_status_check
    check (status in ('pending', 'delivered', 'dead_lettered')),
  add constraint factos_outbox_attempts_check
    check (attempts >= 0),
  add constraint factos_outbox_lock_check
    check (
      (locked_until is null) = (lock_token is null)
      and (lock_token is null or btrim(lock_token) <> '')
    ),
  add constraint factos_outbox_lifecycle_check
    check (
      (status = 'pending'
        and delivered_at is null
        and failed_at is null)
      or
      (status = 'delivered'
        and delivered_at is not null
        and delivered_at >= created_at
        and failed_at is null
        and locked_until is null
        and lock_token is null)
      or
      (status = 'dead_lettered'
        and delivered_at is null
        and failed_at is not null
        and failed_at >= created_at
        and last_error is not null
        and btrim(last_error) <> ''
        and locked_until is null
        and lock_token is null)
    );

drop index if exists factos_outbox_pending;
drop index if exists factos_outbox_lease_pending;

create index factos_outbox_lease_pending
  on factos_outbox(consumer, target, available_at, id)
  where status = 'pending';

-- migrate:down
drop index if exists factos_outbox_lease_pending;

create index factos_outbox_pending
  on factos_outbox(status, available_at, id);

alter table factos_outbox
  drop constraint factos_outbox_lifecycle_check,
  drop constraint factos_outbox_lock_check,
  drop constraint factos_outbox_attempts_check,
  drop constraint factos_outbox_status_check,
  drop column failed_at,
  drop column lock_token;
