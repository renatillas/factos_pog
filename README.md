# factos_pog

`factos_pog` is the PostgreSQL backend for Factos, implemented with
[`pog`](https://hex.pm/packages/pog).

It stores accepted facts in an append-only PostgreSQL event log, reads the facts
relevant to a command, runs your pure `factos.Decider`, and appends new facts
only if the relevant context is still stable.

Use this package when PostgreSQL is your event store and your consistency rules
are expressed with Factos event types and tags.

It persists event records only. It does not maintain materialized views and it
does not execute side effects. Applications build durable read models and effect
delivery on top of the committed records returned by dispatch.

## Install

```toml
[dependencies]
factos = ">= 1.0.0 and < 2.0.0"
factos_pog = ">= 1.0.0 and < 2.0.0"
gleam_erlang = ">= 1.0.0 and < 2.0.0"
pog = ">= 4.1.0 and < 5.0.0"
```

## Set up the schema

The backend ships reusable dbmate-compatible migrations in `priv/dbmate/`.
Application databases should vendor those files into their own migration
repository, commit them, and run them with their normal migration tool before
dispatching commands. The application migration repository owns ordering and
execution history; `factos_pog` owns only the reusable schema artifacts.

In an Erlang-target migration tool, locate the package `priv` directory and copy
the package migrations into your application migration directory:

```gleam
import gleam/erlang/application

let assert Ok(priv_directory) = application.priv_directory("factos_pog")
let migrations_directory = priv_directory <> "/dbmate"
```

`priv/migrations.sql` is retained for compatibility with already-published
versions and for fresh bootstrap examples. Do not treat it as the append-only
migration history for an application database. `factos_pog.migrate` still exists
for v1 compatibility, but it is deprecated.

The migrations create:

- `factos_events`: append-only event rows;
- `factos_event_tags`: indexed tag rows used for context reads;
- `factos_outbox`: durable integration effects produced atomically with events.

## Define a codec

Your domain event type remains yours. PostgreSQL stores opaque bytes plus
queryable metadata, so the application provides an event codec.

```gleam
fn ticket_codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode: encode_event, decode: decode_event)
}
```

The encoder prepares an event for persistence:

```gleam
fn encode_event(event: Event) -> factos_pog.Proposed(Event) {
  case event {
    TicketSold(buyer) ->
      factos_pog.Proposed(
        id: "ticket-sold-" <> buyer,
        event: event,
        type_: factos.event_type("TicketSold"),
        version: 1,
        tags: [factos.tag("event:gleamconf-2026")],
        metadata: factos.empty_metadata(),
        data: bit_array.from_string(buyer),
      )
  }
}
```

The decoder turns stored rows back into domain events:

```gleam
fn decode_event(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(Event), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "TicketSold" -> {
      use buyer <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: TicketSold(buyer),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}
```

Tags are the query contract. If future commands need to find an event by payload
value, expose that value as a tag when writing the event.

## Dispatch commands

`dispatch` is the single write API. Build a dispatch with the required stream,
decider, and codec, then add a context query, reactor, or retry configuration
only when that command needs them. Pass the command as the second argument to
`dispatch`.

```gleam
let builder =
  factos_pog.new_dispatch_builder(
    connection: connection,
    stream: buyer_stream(attempt),
    decider: ticket_decider(),
    codec: ticket_codec(),
  )
  |> factos_pog.with_query(query: sale_query())

let assert Ok(dispatch) = factos_pog.dispatch(builder, BuyTicket(buyer_name(attempt)))
```

The backend:

1. opens a PostgreSQL `SERIALIZABLE` transaction;
2. reads rows matching the builder's query, or the builder's stream when no
   query is configured;
3. decodes and folds them into decision state;
4. runs the decider;
5. appends only if PostgreSQL accepts the serializable transaction and any
   stream revision check still holds;
6. inserts the new events and tag-index rows;
7. inserts outbox effects if the builder has a reactor;
8. retries retryable serialization/deadlock failures up to the builder's retry
   attempts;
9. returns append metadata and committed records.

The return type is:

```gleam
pub type Dispatch(event) {
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}
```

`dispatch.events` contains the committed records inserted by this dispatch. Use
those records for reactors or durable effect adapters.

## Stream-only dispatch

Leave the query unset when one stream revision is intentionally the consistency
boundary:

```gleam
let assert Ok(dispatch) =
  factos_pog.new_dispatch_builder(
    connection: connection,
    stream: "ticket-sale-renata",
    decider: ticket_decider(),
    codec: ticket_codec(),
  )
  |> factos_pog.dispatch(BuyTicket("renata"))
```

Stream-only dispatch remains useful for stream-shaped rules, but a builder with
`with_query` is the better fit when a command depends on facts selected by event
type and tag.

## React after commit

`factos_pog` does not run side effects. It returns committed records so the
application can react explicitly:

```gleam
let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

A reactor is pure. It returns effect values as data. Your application decides how
to execute, persist, retry, or ignore those effects.

## Tradeoff: correctness before throughput

PostgreSQL does not provide a single native primitive for:

> append these events only if no row matching this arbitrary event-type/tag query
> appeared after position N.

`factos_pog` therefore locks the event table while running context dispatch. That
makes arbitrary Factos queries correct, but unrelated writers queue behind each
other. A future backend can use a more granular locking strategy if it preserves
the same append-condition guarantee.

## Example

Run the concurrent ticket sale example:

```sh
cd ../../examples/tickets_pog
docker compose up -d
gleam run
```

The example uses a dispatch builder with `with_query` to protect a capacity rule
across many buyer streams. It then runs a reactor over the committed
`TicketSold` records.
