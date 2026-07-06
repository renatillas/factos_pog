-- migrate:up
create table if not exists factos_locks (
  lock_key text primary key
);

create index if not exists factos_outbox_lease_pending
  on factos_outbox(consumer, target, available_at, id)
  where status = 'pending';

drop index if exists factos_outbox_pending;

-- migrate:down
create index if not exists factos_outbox_pending
  on factos_outbox(status, available_at, id);

drop index if exists factos_outbox_lease_pending;

drop table if exists factos_locks;
