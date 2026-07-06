# How `factos_pog` Works

`factos_pog` is a PostgreSQL event-store backend for the Factos core model.

It exposes one write path: build a `DispatchBuilder` and pass it to
`factos_pog.dispatch`. The builder can protect either:

- one stream revision, when no query is configured;
- an arbitrary Factos event-type/tag context, when `with_query` is used.

The builder can also attach a reactor with `with_reactor`, causing produced
effects to be inserted into `factos_outbox` atomically with the events.

`dispatch` returns `Dispatch(event)`, which includes append metadata and the
committed `factos.Recorded(event)` values inserted by the dispatch.

## Event rows

The append-only table is `factos_events`:

| Column | Meaning |
| --- | --- |
| `position` | Global append order. |
| `id` | Application event id. |
| `stream` | Stream name. |
| `revision` | Per-stream revision. |
| `type` | Store-visible event type name. |
| `version` | Event version from the application codec. |
| `tags` | Newline-encoded store-visible tags. |
| `metadata` | Newline-encoded application metadata. |
| `data` | Opaque application bytes. |

Tags are also mirrored into `factos_event_tags(position, tag)`. This table gives
PostgreSQL indexed predicates for context queries.

## Dispatch flow

A builder without `with_query` is for commands whose consistency boundary is one
stream. A builder with `with_query` is for commands whose consistency boundary is
a `factos.Query`.

The serializable transaction does this:

1. select candidate rows from either the stream or the configured query;
2. decode candidate rows using the application codec;
3. fold events with the decider's `evolve` function;
4. run the decider with the command;
5. check the stream revision or `FailIfEventsMatch(query, after)`;
6. insert produced events;
7. insert tag-index rows;
8. insert outbox effects when the builder has a reactor;
9. return `Dispatch(event)`.

PostgreSQL `SERIALIZABLE` isolation detects concurrent predicate conflicts. The
backend retries retryable serialization/deadlock failures according to the
builder's retry attempts, so deciders and codecs must be pure/idempotent.

## Stream dispatch

Stream-only dispatch remains useful when the business rule really is protected
by one stream. If the rule needs event types and tags across streams, add
`with_query` to the builder.

## Codec boundary

The backend never interprets event payload bytes. The application codec decides:

- how to encode event payloads;
- which event type name to store;
- which tags to expose for future queries;
- how to decode old stored rows;
- how to handle unknown or invalid data.

This keeps the backend generic and makes the query contract visible at write
time.

## Reactors and effects

`dispatch.events` contains committed records, not merely domain events. Records
include id, stream, revision, global position, type, version, tags, metadata, and
payload.

That makes them suitable for `factos.Reactor` values:

```gleam
let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

`factos_pog` does not execute those effects. If effects need retry or durability,
model that in application code or a backend-specific adapter.
