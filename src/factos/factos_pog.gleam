//// PostgreSQL backend for Factos using the `pog` package.
////
//// This backend stores accepted facts in an append-only `factos_events` table.
//// The event history is the source of truth; projections and stream-shaped reads
//// are derived views over that history.
////
//// The context dispatch flow follows the Command Context Consistency idea from
//// "Simply Event Sourcing": a command selects the facts required for its decision,
//// folds them into temporary state, decides new facts, and appends those facts
//// only when no relevant facts appeared after the observed context position.
////
//// The query contract is intentionally tag-based. PostgreSQL stores opaque event
//// bytes, an event type, and tags. This keeps domain serialization outside the
//// backend, but it means any payload value needed for a selective consistency
//// query must be written as a tag.

import factos
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import pog

pub type Proposed(event) {
  /// A domain event prepared for PostgreSQL persistence.
  ///
  /// The application codec creates this value. `id` should identify the event for
  /// the application. `type_` and `tags` are store-visible query metadata. `data`
  /// is opaque bytes owned by the application codec.
  Proposed(
    id: String,
    event: event,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: BitArray,
  )
}

pub type StoredEvent {
  /// A raw event row read from PostgreSQL before domain decoding.
  ///
  /// Decoders receive this value so they can inspect stored metadata and bytes.
  /// `position` is the global append order. `revision` is the per-stream revision.
  StoredEvent(
    position: Int,
    id: String,
    stream: String,
    revision: Int,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: BitArray,
  )
}

pub type EventCodec(event) {
  /// Application-owned PostgreSQL event codec.
  ///
  /// `encode` converts a domain event into bytes and metadata. `decode` converts a
  /// stored row back into a `factos.Decoded` domain event. Decode failures are kept
  /// in the application's own error type and wrapped as `DecodeError`.
  ///
  /// WARNING: codecs used by dispatch must be pure.
  ///
  /// Serializable dispatch can retry the same command after a transaction
  /// conflict. That can call `encode` and `decode` more than once for the same
  /// logical operation, so codec functions must not perform IO, mutate external
  /// state, allocate ids from an external system, publish messages, or otherwise
  /// create host-system side effects.
  ///
  /// Return deterministic stored bytes and metadata from input values only. Put
  /// external effects in durable outbox records and execute them after commit.
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(StoredEvent) -> Result(factos.Decoded(event), DecodeError),
  )
}

pub type Append {
  /// Result of a successful append.
  ///
  /// `current_revision` is the latest revision of the target stream after the
  /// append. `position` is the global position of the last inserted event, or
  /// `NoPosition` when no events were produced.
  Append(current_revision: Int, position: factos.SequencePosition)
}

pub type Dispatch(event) {
  /// Result of a successful dispatch.
  ///
  /// `append` has the stream revision and final global position. `events` are the
  /// committed events recorded by this dispatch, suitable for pure Factos
  /// reactors or backend-specific durable effect adapters.
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}

pub type ProposedEffect(effect) {
  /// A durable integration effect prepared for outbox persistence.
  ///
  /// Applications own the effect payload and type names. Factos stores the
  /// delivery envelope so workers can lease, retry, and acknowledge effects
  /// without knowing the domain payload.
  ProposedEffect(
    effect: effect,
    consumer: String,
    key: String,
    target: String,
    type_: String,
    metadata: factos.Metadata,
    payload: BitArray,
  )
}

pub type EffectCodec(effect) {
  /// Application-owned outbox effect codec.
  ///
  /// WARNING: effect codecs used by dispatch must be pure.
  ///
  /// Serializable dispatch can retry the same command after a transaction
  /// conflict. That can call `encode` more than once for the same effect value,
  /// so this function must only convert an effect into a deterministic durable
  /// outbox envelope. It must not execute the effect, publish messages, call
  /// external services, mutate state, or allocate externally-visible ids.
  EffectCodec(encode: fn(effect) -> ProposedEffect(effect))
}

pub opaque type DispatchBuilder(command, state, event, domain_error, effect) {
  DispatchBuilder(
    connection: pog.Connection,
    stream: String,
    query: DispatchQuery,
    decider: factos.Decider(command, state, event, domain_error),
    event_codec: EventCodec(event),
    effects: DispatchEffects(event, effect),
    retry_attempts: Int,
  )
}

type DispatchQuery {
  StreamQuery
  ContextQuery(factos.Query)
}

type DispatchEffects(event, effect) {
  NoEffects
  ReactorEffects(
    reactor: factos.Reactor(event, effect),
    effect_codec: EffectCodec(effect),
  )
}

/// Start building an event dispatch.
///
/// By default the builder uses one-stream consistency, no reactor effects, and
/// 100 attempts for retryable serializable transaction conflicts.
///
/// WARNING: every function passed into dispatch must be pure.
///
/// Dispatch runs inside retryable serializable transactions. When PostgreSQL
/// reports a retryable conflict, Factos may call the decider, codec, query
/// evolve function, and reactor derivation more than once for the same command.
/// Any side effect performed by those functions can therefore happen multiple
/// times in the host system.
///
/// Keep command handling, event encoding and decoding, state evolution, and
/// reactor derivation deterministic and side-effect free. Put external effects
/// in durable outbox records and execute them after the transaction commits.
pub fn new_dispatch(
  connection connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
) -> DispatchBuilder(command, state, event, domain_error, effect) {
  DispatchBuilder(
    connection: connection,
    stream: stream_name,
    query: StreamQuery,
    decider: decider,
    event_codec: codec,
    effects: NoEffects,
    retry_attempts: 5,
  )
}

/// Use a context query instead of one-stream consistency.
pub fn with_query(
  builder: DispatchBuilder(command, state, event, domain_error, effect),
  query query: factos.Query,
) -> DispatchBuilder(command, state, event, domain_error, effect) {
  let DispatchBuilder(
    connection:,
    stream:,
    decider:,
    event_codec:,
    effects:,
    retry_attempts:,
    ..,
  ) = builder
  DispatchBuilder(
    connection:,
    stream:,
    query: ContextQuery(query),
    decider:,
    event_codec:,
    effects:,
    retry_attempts:,
  )
}

/// Persist reactor effects into the outbox in the same transaction as events.
pub fn with_reactor(
  builder: DispatchBuilder(command, state, event, domain_error, old_effect),
  reactor reactor: factos.Reactor(event, effect),
  codec effect_codec: EffectCodec(effect),
) -> DispatchBuilder(command, state, event, domain_error, effect) {
  let DispatchBuilder(
    connection:,
    stream:,
    query:,
    decider:,
    event_codec:,
    retry_attempts:,
    ..,
  ) = builder
  DispatchBuilder(
    connection:,
    stream:,
    query:,
    decider:,
    event_codec:,
    effects: ReactorEffects(reactor:, effect_codec:),
    retry_attempts:,
  )
}

/// Override retry attempts for PostgreSQL serializable/deadlock conflicts.
pub fn with_retry_attempts(
  builder: DispatchBuilder(command, state, event, domain_error, effect),
  attempts attempts: Int,
) -> DispatchBuilder(command, state, event, domain_error, effect) {
  let DispatchBuilder(
    connection:,
    stream:,
    query:,
    decider:,
    event_codec:,
    effects:,
    ..,
  ) = builder
  DispatchBuilder(
    connection:,
    stream:,
    query:,
    decider:,
    event_codec:,
    effects:,
    retry_attempts: int.max(attempts, 1),
  )
}

/// Dispatch a command with a configured builder.
pub fn dispatch(
  builder: DispatchBuilder(command, state, event, domain_error, effect),
  command: command,
) -> Result(Dispatch(event), Error(domain_error)) {
  let DispatchBuilder(
    connection:,
    stream:,
    query:,
    decider:,
    event_codec:,
    effects:,
    retry_attempts:,
  ) = builder

  case query, effects {
    StreamQuery, NoEffects ->
      dispatch_stream(
        connection,
        stream:,
        decider:,
        codec: event_codec,
        command:,
        retry_attempts:,
      )
    ContextQuery(query), NoEffects ->
      dispatch_query(
        connection,
        stream:,
        query:,
        decider:,
        codec: event_codec,
        command:,
        retry_attempts:,
      )
    StreamQuery, ReactorEffects(reactor:, effect_codec:) ->
      dispatch_stream_with_reactor(
        connection,
        stream:,
        decider:,
        event_codec:,
        command:,
        reactor:,
        effect_codec:,
        retry_attempts:,
      )
    ContextQuery(query), ReactorEffects(reactor:, effect_codec:) ->
      dispatch_query_with_reactor(
        connection,
        stream:,
        query:,
        decider:,
        event_codec:,
        command:,
        reactor:,
        effect_codec:,
        retry_attempts:,
      )
  }
}

pub type OutboxMessage {
  /// A leased outbox message ready for delivery by an application worker.
  OutboxMessage(
    id: Int,
    source_position: factos.SequencePosition,
    source_event_id: String,
    source_context: String,
    consumer: String,
    key: String,
    target: String,
    type_: String,
    metadata: factos.Metadata,
    payload: BitArray,
    /// Unguessable identity of this specific lease acquisition.
    lock_token: String,
  )
}

/// Outcome of a guarded outbox finalization attempt.
pub type OutboxFinalization {
  /// The caller held the current, unexpired lease and changed the row.
  Finalized
  /// The row is no longer pending under the supplied current lease token.
  StaleLease
}

pub type Error(domain_error) {
  /// The decider rejected the command with a domain error.
  DomainError(domain_error)

  /// PostgreSQL or `pog` returned an error while running a query.
  StoreError(pog.QueryError)

  /// A stream revision or context append condition failed.
  AppendConditionFailed(factos.AppendCondition)
  DecodeError(DecodeError)
}

pub type DecodeError {
  UnknownEvent
  InvalidData
}

type QuerySql {
  QuerySql(sql: String, parameters: List(QueryParameter))
}

type QueryParameter {
  IntParameter(Int)
  TextParameter(String)
}

/// Create a new codec.
///
/// `encode` turns a domain event into a `Proposed` event ready for persistence.
/// `decode` turns a stored row back into a domain event. Decode failures are
/// returned as `DecodeError` and stop load/read flows rather than panicking.
///
/// WARNING: codecs used by dispatch must be pure. Serializable dispatch may
/// retry and call `encode` or `decode` more than once for the same logical
/// operation. Codec functions must be deterministic conversions only; external
/// side effects belong in durable outbox records after commit.
pub fn codec(
  encode encode: fn(event) -> Proposed(event),
  decode decode: fn(StoredEvent) -> Result(factos.Decoded(event), DecodeError),
) -> EventCodec(event) {
  EventCodec(encode:, decode:)
}

/// Create an effect codec.
///
/// WARNING: effect codecs used by dispatch must be pure. Serializable dispatch
/// may retry and call `encode` more than once for the same effect value. This
/// function must only build a deterministic durable outbox envelope; it must not
/// execute the effect or perform any other host-system side effect.
pub fn effect_codec(
  encode encode: fn(effect) -> ProposedEffect(effect),
) -> EffectCodec(effect) {
  EffectCodec(encode:)
}

/// Read and fold the facts selected by a command-context query.
///
/// The backend reads stored rows, decodes them with the supplied codec, filters
/// them with `factos.matches_query`, folds matching events with the decider's
/// `evolve` function, and returns a `factos.Context` with a
/// `FailIfEventsMatch(query, after)` append condition.
pub fn read_context(
  connection: pog.Connection,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
) -> Result(factos.Context(event, state), Error(domain_error)) {
  let factos.Decider(initial, _, evolve) = decider

  use events <- result.try(read_matching_events(connection, query, codec))
  let position = highest_recorded_position(events)

  Ok(factos.Context(
    query:,
    state: factos.evolve_recorded(
      initial: initial,
      events: events,
      evolve: evolve,
    ),
    events: events,
    position: position,
    append_condition: factos.FailIfEventsMatch(query, position),
  ))
}

/// Read committed events after a global sequence position.
///
/// This is the bounded polling primitive used by durable subscriptions and
/// process managers. Events are returned in global append order.
pub fn read_events_after(
  connection: pog.Connection,
  query query: factos.Query,
  after after: factos.SequencePosition,
  limit limit: Int,
  codec codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  case limit <= 0 {
    True -> Ok([])
    False -> {
      let QuerySql(where_sql, parameters) =
        matching_events_after_sql(query, after)
      use rows <- result.try(
        pog.query(
          "select position, id, stream, revision, type, version, tags, metadata, data
           from factos_events
           "
          <> where_sql
          <> "
           order by position
           limit "
          <> int.to_string(limit),
        )
        |> with_parameters(parameters)
        |> pog.returning(stored_event_decoder())
        |> pog.execute(on: connection)
        |> result.map(fn(returned) { returned.rows })
        |> result.map_error(StoreError),
      )
      decode_rows(rows, codec)
    }
  }
}

fn dispatch_query(
  connection: pog.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
  command command: command,
  retry_attempts retry_attempts: Int,
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_serializable_transaction(
    connection,
    retry_attempts,
  )
  use context <- result.try(read_context(
    transaction_connection,
    query: query,
    decider: decider,
    codec: codec,
  ))
  use pair <- result.try(
    factos.decide_context(context, command, decider)
    |> result.map_error(DomainError),
  )
  let #(context, events) = pair

  append_context_events(
    transaction_connection,
    stream_name,
    events,
    codec,
    context.append_condition,
  )
}

fn dispatch_query_with_reactor(
  connection: pog.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  event_codec event_codec: EventCodec(event),
  command command: command,
  reactor reactor: factos.Reactor(event, effect),
  effect_codec effect_codec: EffectCodec(effect),
  retry_attempts retry_attempts: Int,
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_serializable_transaction(
    connection,
    retry_attempts,
  )
  use context <- result.try(read_context(
    transaction_connection,
    query: query,
    decider: decider,
    codec: event_codec,
  ))
  use pair <- result.try(
    factos.decide_context(context, command, decider)
    |> result.map_error(DomainError),
  )
  let #(context, events) = pair
  use dispatch <- result.try(append_context_events(
    transaction_connection,
    stream_name,
    events,
    event_codec,
    context.append_condition,
  ))
  use _ <- result.try(insert_outbox_effects(
    transaction_connection,
    dispatch.events,
    reactor,
    effect_codec,
  ))

  Ok(dispatch)
}

/// Load and fold one stream.
///
/// This supports classic stream-revision consistency. The returned
/// `factos.LoadedStream` contains the folded state, decoded recorded events, and
/// current stream revision.
pub fn load_stream(
  connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
) -> Result(factos.LoadedStream(event, state), Error(domain_error)) {
  let factos.Decider(initial, _, evolve) = decider
  use events <- result.try(read_stream_events(connection, stream_name, codec))

  Ok(factos.LoadedStream(
    stream: stream_name,
    state: factos.evolve_recorded(
      initial: initial,
      events: events,
      evolve: evolve,
    ),
    events: events,
    revision: stream_revision(events),
  ))
}

fn dispatch_stream(
  connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
  command command: command,
  retry_attempts retry_attempts: Int,
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_serializable_transaction(
    connection,
    retry_attempts,
  )
  use loaded <- result.try(load_stream(
    transaction_connection,
    stream: stream_name,
    decider: decider,
    codec: codec,
  ))
  let factos.Decider(_, decide, _) = decider
  use events <- result.try(
    decide(loaded.state, command)
    |> result.map_error(DomainError),
  )

  append_stream_events(
    transaction_connection,
    stream_name,
    events,
    codec,
    loaded.revision,
    factos.NoAppendCondition,
  )
}

fn dispatch_stream_with_reactor(
  connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  event_codec event_codec: EventCodec(event),
  command command: command,
  reactor reactor: factos.Reactor(event, effect),
  effect_codec effect_codec: EffectCodec(effect),
  retry_attempts retry_attempts: Int,
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_serializable_transaction(
    connection,
    retry_attempts,
  )
  use loaded <- result.try(load_stream(
    transaction_connection,
    stream: stream_name,
    decider: decider,
    codec: event_codec,
  ))
  let factos.Decider(_, decide, _) = decider
  use events <- result.try(
    decide(loaded.state, command)
    |> result.map_error(DomainError),
  )
  use dispatch <- result.try(append_stream_events(
    transaction_connection,
    stream_name,
    events,
    event_codec,
    loaded.revision,
    factos.NoAppendCondition,
  ))
  use _ <- result.try(insert_outbox_effects(
    transaction_connection,
    dispatch.events,
    reactor,
    effect_codec,
  ))

  Ok(dispatch)
}

fn run_serializable_transaction(
  connection: pog.Connection,
  retry_attempts: Int,
  work: fn(pog.Connection) -> Result(Dispatch(event), Error(domain_error)),
) -> Result(Dispatch(event), Error(domain_error)) {
  run_serializable_transaction_attempt(
    connection,
    work,
    attempts_remaining: retry_attempts,
  )
}

fn run_serializable_transaction_attempt(
  connection: pog.Connection,
  work: fn(pog.Connection) -> Result(Dispatch(event), Error(domain_error)),
  attempts_remaining attempts_remaining: Int,
) -> Result(Dispatch(event), Error(domain_error)) {
  let result = run_serializable_transaction_once(connection, work)
  case result {
    Ok(dispatch) -> Ok(dispatch)
    Error(error) -> {
      case attempts_remaining > 1 && retryable_transaction_error(error) {
        True ->
          run_serializable_transaction_attempt(
            connection,
            work,
            attempts_remaining: attempts_remaining - 1,
          )
        False -> Error(error)
      }
    }
  }
}

fn run_serializable_transaction_once(
  connection: pog.Connection,
  work: fn(pog.Connection) -> Result(Dispatch(event), Error(domain_error)),
) -> Result(Dispatch(event), Error(domain_error)) {
  case
    {
      use transaction_connection <- pog.transaction(connection)
      use _ <- result.try(set_serializable_isolation(transaction_connection))
      work(transaction_connection)
    }
  {
    Ok(dispatch) -> Ok(dispatch)
    Error(pog.TransactionQueryError(error)) -> Error(StoreError(error))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

fn set_serializable_isolation(
  connection: pog.Connection,
) -> Result(Nil, Error(_)) {
  pog.query("set transaction isolation level serializable")
  |> pog.execute(on: connection)
  |> result.map(nil_constant)
  |> result.map_error(StoreError)
}

fn retryable_transaction_error(error: Error(_)) -> Bool {
  case error {
    StoreError(pog.PostgresqlError(code: "40001", ..)) -> True
    StoreError(pog.PostgresqlError(code: "40P01", ..)) -> True
    _ -> False
  }
}

fn append_context_events(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
  condition: factos.AppendCondition,
) -> Result(Dispatch(event), Error(domain_error)) {
  use revision <- result.try(
    current_revision(connection, stream_name)
    |> result.map_error(StoreError),
  )
  append_stream_events(
    connection,
    stream_name,
    events,
    codec,
    factos.CurrentRevision(revision),
    condition,
  )
}

fn append_stream_events(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
  expected: factos.Revision,
  condition: factos.AppendCondition,
) -> Result(Dispatch(event), Error(domain_error)) {
  case events {
    [] -> {
      let append =
        Append(
          current_revision: revision_to_int(expected),
          position: factos.NoPosition,
        )
      Ok(Dispatch(append:, events: []))
    }
    [_, ..] -> {
      insert_events(
        connection,
        stream_name,
        events,
        codec,
        revision_to_int(expected) + 1,
        expected,
        condition,
        factos.NoPosition,
        [],
      )
    }
  }
}

fn map_event_insert_error(error: pog.QueryError) -> Error(domain_error) {
  case error {
    pog.ConstraintViolated(constraint: "factos_events_stream_revision_key", ..) ->
      AppendConditionFailed(factos.NoAppendCondition)
    _ -> StoreError(error)
  }
}

fn insert_events(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
  revision: Int,
  expected: factos.Revision,
  condition: factos.AppendCondition,
  position: factos.SequencePosition,
  recorded_events: List(factos.Recorded(event)),
) -> Result(Dispatch(event), Error(domain_error)) {
  case events {
    [] -> {
      let append = Append(current_revision: revision - 1, position: position)
      Ok(Dispatch(append:, events: list.reverse(recorded_events)))
    }
    [event, ..rest] -> {
      let EventCodec(encode:, ..) = codec
      let Proposed(id, _, type_, version, tags, metadata, data) = encode(event)
      use returned_position <- result.try(insert_event_if_revision_matches(
        connection,
        stream_name,
        revision:,
        expected:,
        condition:,
        id:,
        type_:,
        version:,
        tags:,
        metadata:,
        data:,
      ))
      let position = factos.SequencePosition(returned_position)
      let recorded =
        factos.Recorded(
          id: id,
          stream: stream_name,
          revision:,
          position:,
          type_:,
          version:,
          tags:,
          metadata:,
          event:,
        )
      use _ <- result.try(insert_event_tags(connection, position, tags))
      insert_events(
        connection,
        stream_name,
        rest,
        codec,
        revision + 1,
        factos.CurrentRevision(revision),
        factos.NoAppendCondition,
        position,
        [recorded, ..recorded_events],
      )
    }
  }
}

fn insert_event_if_revision_matches(
  connection: pog.Connection,
  stream_name: String,
  revision revision: Int,
  expected expected: factos.Revision,
  condition condition: factos.AppendCondition,
  id id: String,
  type_ type_: factos.EventType,
  version version: Int,
  tags tags: List(factos.Tag),
  metadata metadata: factos.Metadata,
  data data: BitArray,
) -> Result(Int, Error(domain_error)) {
  let QuerySql(condition_sql, condition_parameters) =
    append_condition_to_having_sql(condition)
  use returned <- result.try(
    pog.query("
      insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
      select $1, $2, $3, $4, $5, $6, $7, $8
      from factos_events
      where stream = $2
      having coalesce(max(revision), -1) = $9
      " <> condition_sql <> "
      returning position
      ")
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.text(stream_name))
    |> pog.parameter(pog.int(revision))
    |> pog.parameter(pog.text(factos.event_type_name(type_)))
    |> pog.parameter(pog.int(version))
    |> pog.parameter(pog.text(tags_to_text(tags)))
    |> pog.parameter(pog.text(metadata_to_text(metadata)))
    |> pog.parameter(pog.bytea(data))
    |> pog.parameter(pog.int(revision_to_int(expected)))
    |> with_parameters(condition_parameters)
    |> pog.returning(int_field_decoder())
    |> pog.execute(on: connection)
    |> result.map_error(map_event_insert_error),
  )

  case returned.rows {
    [position, ..] -> Ok(position)
    [] -> Error(AppendConditionFailed(condition))
  }
}

fn append_condition_to_having_sql(
  condition: factos.AppendCondition,
) -> QuerySql {
  case condition {
    factos.NoAppendCondition -> QuerySql(sql: "", parameters: [])
    factos.FailIfEventsMatch(query, after) -> {
      let QuerySql(where_sql, parameters) =
        matching_events_after_sql_from(query, after, parameter_index: 10)
      QuerySql(sql: " and not exists (
          select 1
          from factos_events
          " <> where_sql <> "
        )", parameters: parameters)
    }
  }
}

fn insert_outbox_effects(
  connection: pog.Connection,
  events: List(factos.Recorded(event)),
  reactor: factos.Reactor(event, effect),
  effect_codec: EffectCodec(effect),
) -> Result(Nil, Error(domain_error)) {
  case events {
    [] -> Ok(Nil)
    [event, ..rest] -> {
      use _ <- result.try(insert_outbox_effects_for_event(
        connection,
        event,
        factos.react(reactor, event),
        effect_codec,
      ))
      insert_outbox_effects(connection, rest, reactor, effect_codec)
    }
  }
}

fn insert_outbox_effects_for_event(
  connection: pog.Connection,
  event: factos.Recorded(event),
  effects: List(effect),
  effect_codec: EffectCodec(effect),
) -> Result(Nil, Error(domain_error)) {
  case effects {
    [] -> Ok(Nil)
    [effect, ..rest] -> {
      use _ <- result.try(insert_outbox_effect(
        connection,
        event,
        effect,
        effect_codec,
      ))
      insert_outbox_effects_for_event(connection, event, rest, effect_codec)
    }
  }
}

fn insert_outbox_effect(
  connection: pog.Connection,
  event: factos.Recorded(event),
  effect: effect,
  effect_codec: EffectCodec(effect),
) -> Result(Nil, Error(domain_error)) {
  let EffectCodec(encode) = effect_codec
  let ProposedEffect(_, consumer, key, target, type_, metadata, payload) =
    encode(effect)
  let source_position = case event.position {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }

  pog.query(
    "
    insert into factos_outbox (
      source_position,
      source_event_id,
      source_context,
      consumer,
      effect_key,
      target,
      type,
      metadata,
      payload
    )
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    on conflict (consumer, effect_key) do nothing
    ",
  )
  |> pog.parameter(pog.int(source_position))
  |> pog.parameter(pog.text(event.id))
  |> pog.parameter(pog.text(event.stream))
  |> pog.parameter(pog.text(consumer))
  |> pog.parameter(pog.text(key))
  |> pog.parameter(pog.text(target))
  |> pog.parameter(pog.text(type_))
  |> pog.parameter(pog.text(metadata_to_text(metadata)))
  |> pog.parameter(pog.bytea(payload))
  |> pog.execute(on: connection)
  |> result.map(nil_constant)
  |> result.map_error(StoreError)
}

fn read_matching_events(
  connection: pog.Connection,
  query: factos.Query,
  codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  let QuerySql(where_sql, parameters) = query_to_sql(query, 1)
  use rows <- result.try(
    pog.query(
      "select position, id, stream, revision, type, version, tags, metadata, data
       from factos_events
       "
      <> where_sql
      <> "
       order by position",
    )
    |> with_parameters(parameters)
    |> pog.returning(stored_event_decoder())
    |> pog.execute(on: connection)
    |> result.map(fn(returned) { returned.rows })
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
}

fn read_stream_events(
  connection: pog.Connection,
  stream_name: String,
  codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  use rows <- result.try(
    pog.query(
      "select position, id, stream, revision, type, version, tags, metadata, data from factos_events where stream = $1 order by revision",
    )
    |> pog.parameter(pog.text(stream_name))
    |> pog.returning(stored_event_decoder())
    |> pog.execute(on: connection)
    |> result.map(fn(returned) { returned.rows })
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
}

fn decode_rows(
  rows: List(StoredEvent),
  codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  case rows {
    [] -> Ok([])
    [row, ..rest] -> {
      use recorded <- result.try(decode_row(row, codec))
      use rest <- result.try(decode_rows(rest, codec))
      Ok([recorded, ..rest])
    }
  }
}

fn decode_row(
  row: StoredEvent,
  codec: EventCodec(event),
) -> Result(factos.Recorded(event), Error(domain_error)) {
  let EventCodec(_, decode_event) = codec
  use decoded <- result.try(decode_event(row) |> result.map_error(DecodeError))
  let factos.Decoded(event, type_, version, tags, metadata) = decoded
  let StoredEvent(position, id, stream, revision, _, _, _, _, _) = row

  Ok(factos.Recorded(
    id: id,
    stream: stream,
    revision: revision,
    position: factos.SequencePosition(position),
    type_: type_,
    version: version,
    tags: tags,
    metadata: metadata,
    event: event,
  ))
}

fn stored_event_decoder() -> decode.Decoder(StoredEvent) {
  use position <- decode.field(0, decode.int)
  use id <- decode.field(1, decode.string)
  use stream <- decode.field(2, decode.string)
  use revision <- decode.field(3, decode.int)
  use type_name <- decode.field(4, decode.string)
  use version <- decode.field(5, decode.int)
  use tags <- decode.field(6, decode.string)
  use metadata <- decode.field(7, decode.string)
  use data <- decode.field(8, decode.bit_array)
  decode.success(StoredEvent(
    position: position,
    id: id,
    stream: stream,
    revision: revision,
    type_: factos.event_type(type_name),
    version: version,
    tags: tags_from_text(tags),
    metadata: metadata_from_text(metadata),
    data: data,
  ))
}

fn current_revision(
  connection: pog.Connection,
  stream_name: String,
) -> Result(Int, pog.QueryError) {
  use returned <- result.try(
    pog.query(
      "select coalesce(max(revision), -1) from factos_events where stream = $1",
    )
    |> pog.parameter(pog.text(stream_name))
    |> pog.returning(int_field_decoder())
    |> pog.execute(on: connection),
  )

  case returned.rows {
    [revision, ..] -> Ok(revision)
    [] -> Ok(-1)
  }
}

fn int_field_decoder() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}

fn stream_revision(events: List(factos.Recorded(event))) -> factos.Revision {
  case list.reverse(events) {
    [] -> factos.NoEvents
    [event, ..] -> factos.CurrentRevision(event.revision)
  }
}

fn highest_recorded_position(
  events: List(factos.Recorded(event)),
) -> factos.SequencePosition {
  case list.reverse(events) {
    [] -> factos.NoPosition
    [event, ..] -> event.position
  }
}

fn revision_to_int(revision: factos.Revision) -> Int {
  case revision {
    factos.NoEvents -> -1
    factos.CurrentRevision(revision) -> revision
  }
}

fn insert_event_tags(
  connection: pog.Connection,
  position: factos.SequencePosition,
  tags: List(factos.Tag),
) -> Result(Nil, Error(domain_error)) {
  case position, tags {
    factos.NoPosition, _ -> Ok(Nil)
    _, [] -> Ok(Nil)
    factos.SequencePosition(position), [_, ..] -> {
      use _ <- result.try(
        pog.query(
          "
          insert into factos_event_tags(position, tag)
          select $1, unnest($2::text[])
          on conflict do nothing
          ",
        )
        |> pog.parameter(pog.int(position))
        |> pog.parameter(pog.array(
          fn(tag) { pog.text(factos.tag_value(tag)) },
          tags,
        ))
        |> pog.execute(on: connection)
        |> result.map_error(StoreError),
      )
      Ok(Nil)
    }
  }
}

fn query_to_sql(query: factos.Query, parameter_index: Int) -> QuerySql {
  case query {
    factos.AllEvents -> QuerySql(sql: "", parameters: [])
    factos.Query(items) -> {
      case items {
        [] -> QuerySql(sql: "where 1 = 0", parameters: [])
        [_, ..] -> {
          let #(sql, parameters, _) =
            build_query_items_sql(items, parameter_index, [], [])
          QuerySql(
            sql: "where " <> string.join(list.reverse(sql), with: " or "),
            parameters: list.reverse(parameters),
          )
        }
      }
    }
  }
}

fn matching_events_after_sql(
  query: factos.Query,
  after: factos.SequencePosition,
) -> QuerySql {
  matching_events_after_sql_from(query, after, parameter_index: 1)
}

fn matching_events_after_sql_from(
  query: factos.Query,
  after: factos.SequencePosition,
  parameter_index parameter_index: Int,
) -> QuerySql {
  let after_position = case after {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }
  let after_placeholder = "$" <> int.to_string(parameter_index)

  case query {
    factos.AllEvents ->
      QuerySql(sql: "where position > " <> after_placeholder, parameters: [
        IntParameter(after_position),
      ])
    factos.Query(items) -> {
      case items {
        [] -> QuerySql(sql: "where 1 = 0", parameters: [])
        [_, ..] -> {
          let #(sql, parameters, _) =
            build_query_items_sql(items, parameter_index + 1, [], [
              IntParameter(after_position),
            ])
          QuerySql(
            sql: "where position > "
              <> after_placeholder
              <> " and ("
              <> string.join(list.reverse(sql), with: " or ")
              <> ")",
            parameters: list.reverse(parameters),
          )
        }
      }
    }
  }
}

fn build_query_items_sql(
  items: List(factos.QueryItem),
  parameter_index: Int,
  sql: List(String),
  parameters: List(QueryParameter),
) -> #(List(String), List(QueryParameter), Int) {
  case items {
    [] -> #(sql, parameters, parameter_index)
    [item, ..rest] -> {
      let QuerySql(item_sql, item_parameters) =
        query_item_to_sql(item, parameter_index)
      build_query_items_sql(
        rest,
        parameter_index + list.length(item_parameters),
        [item_sql, ..sql],
        list.append(list.reverse(item_parameters), parameters),
      )
    }
  }
}

fn query_item_to_sql(item: factos.QueryItem, parameter_index: Int) -> QuerySql {
  let factos.QueryItem(types, tags) = item
  let QuerySql(type_sql, type_parameters) = types_to_sql(types, parameter_index)
  let QuerySql(tag_sql, tag_parameters) =
    tags_to_sql(tags, parameter_index + list.length(type_parameters))

  QuerySql(
    sql: "(" <> type_sql <> " and " <> tag_sql <> ")",
    parameters: list.append(type_parameters, tag_parameters),
  )
}

fn types_to_sql(
  types: List(factos.EventType),
  parameter_index: Int,
) -> QuerySql {
  case types {
    [] -> QuerySql(sql: "1 = 1", parameters: [])
    [_, ..] ->
      QuerySql(
        sql: "type in ("
          <> placeholders(parameter_index, list.length(types))
          <> ")",
        parameters: list.map(types, fn(type_) {
          TextParameter(factos.event_type_name(type_))
        }),
      )
  }
}

fn tags_to_sql(tags: List(factos.Tag), parameter_index: Int) -> QuerySql {
  case tags {
    [] -> QuerySql(sql: "1 = 1", parameters: [])
    [_, ..] -> {
      let clauses =
        tags
        |> list.index_map(fn(_tag, index) { "exists (
            select 1 from factos_event_tags
            where factos_event_tags.position = factos_events.position
              and factos_event_tags.tag = $" <> int.to_string(
            parameter_index + index,
          ) <> "
          )" })
      QuerySql(
        sql: "(" <> string.join(clauses, with: " and ") <> ")",
        parameters: list.map(tags, fn(tag) {
          TextParameter(factos.tag_value(tag))
        }),
      )
    }
  }
}

fn placeholders(start: Int, count: Int) -> String {
  placeholder_indices(start, count, [])
  |> list.map(int.to_string)
  |> list.map(fn(index) { "$" <> index })
  |> string.join(with: ", ")
}

fn placeholder_indices(
  start: Int,
  remaining: Int,
  acc: List(Int),
) -> List(Int) {
  case remaining <= 0 {
    True -> list.reverse(acc)
    False -> placeholder_indices(start + 1, remaining - 1, [start, ..acc])
  }
}

fn with_parameters(
  query: pog.Query(row),
  parameters: List(QueryParameter),
) -> pog.Query(row) {
  case parameters {
    [] -> query
    [parameter, ..rest] -> {
      let query = case parameter {
        IntParameter(value) -> pog.parameter(query, pog.int(value))
        TextParameter(value) -> pog.parameter(query, pog.text(value))
      }
      with_parameters(query, rest)
    }
  }
}

/// Lease pending outbox messages for a consumer and target.
///
/// Leased messages stay in `pending` status but are hidden from competing
/// workers until `locked_until` expires.
pub fn lease_outbox(
  connection: pog.Connection,
  consumer consumer: String,
  target target: String,
  limit limit: Int,
  lease_for_milliseconds lease_for_milliseconds: Int,
) -> Result(List(OutboxMessage), Error(_)) {
  case limit <= 0 {
    True -> Ok([])
    False ->
      pog.query(
        "
        update factos_outbox
        set
          locked_until = now() + ($4::integer * interval '1 millisecond'),
          lock_token = gen_random_uuid()::text,
          attempts = attempts + 1
        where id in (
          select id
          from factos_outbox
          where consumer = $1
            and target = $2
            and status = 'pending'
            and available_at <= now()
            and (locked_until is null or locked_until <= now())
          order by available_at, id
          limit $3
          for update skip locked
        )
        returning
          id,
          source_position,
          source_event_id,
          source_context,
          consumer,
          effect_key,
          target,
          type,
          metadata,
          payload,
          lock_token
        ",
      )
      |> pog.parameter(pog.text(consumer))
      |> pog.parameter(pog.text(target))
      |> pog.parameter(pog.int(limit))
      |> pog.parameter(pog.int(lease_for_milliseconds))
      |> pog.returning(outbox_message_decoder())
      |> pog.execute(on: connection)
      |> result.map(fn(returned) { returned.rows })
      |> result.map_error(StoreError)
  }
}

/// Mark an outbox message as delivered if the caller still owns its lease.
pub fn ack_outbox(
  connection: pog.Connection,
  id id: Int,
  lock_token lock_token: String,
) -> Result(OutboxFinalization, Error(_)) {
  pog.query(
    "
    update factos_outbox
    set status = 'delivered',
        delivered_at = now(),
        locked_until = null,
        lock_token = null
    where id = $1
      and lock_token = $2
      and status = 'pending'
      and locked_until > now()
    returning id
    ",
  )
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.text(lock_token))
  |> pog.returning(int_field_decoder())
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [_, ..] -> Finalized
      [] -> StaleLease
    }
  })
  |> result.map_error(StoreError)
}

/// Release an outbox message for retry if the caller still owns its lease.
pub fn nack_outbox(
  connection: pog.Connection,
  id id: Int,
  lock_token lock_token: String,
  error error: String,
  retry_after_milliseconds retry_after_milliseconds: Int,
) -> Result(OutboxFinalization, Error(_)) {
  pog.query(
    "
    update factos_outbox
    set locked_until = null,
        lock_token = null,
        last_error = $3,
        available_at = now() + ($4::integer * interval '1 millisecond')
    where id = $1
      and lock_token = $2
      and status = 'pending'
      and locked_until > now()
    returning id
    ",
  )
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.text(lock_token))
  |> pog.parameter(pog.text(error))
  |> pog.parameter(pog.int(retry_after_milliseconds))
  |> pog.returning(int_field_decoder())
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [_, ..] -> Finalized
      [] -> StaleLease
    }
  })
  |> result.map_error(StoreError)
}

/// Permanently dead-letter a poison message if the caller owns its lease.
///
/// `error` is required and the database rejects empty or whitespace-only values.
pub fn fail_outbox(
  connection: pog.Connection,
  id id: Int,
  lock_token lock_token: String,
  error error: String,
) -> Result(OutboxFinalization, Error(_)) {
  pog.query(
    "
    update factos_outbox
    set status = 'dead_lettered',
        failed_at = now(),
        last_error = $3,
        locked_until = null,
        lock_token = null
    where id = $1
      and lock_token = $2
      and status = 'pending'
      and locked_until > now()
    returning id
    ",
  )
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.text(lock_token))
  |> pog.parameter(pog.text(error))
  |> pog.returning(int_field_decoder())
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [_, ..] -> Finalized
      [] -> StaleLease
    }
  })
  |> result.map_error(StoreError)
}

fn outbox_message_decoder() -> decode.Decoder(OutboxMessage) {
  use id <- decode.field(0, decode.int)
  use source_position <- decode.field(1, decode.int)
  use source_event_id <- decode.field(2, decode.string)
  use source_context <- decode.field(3, decode.string)
  use consumer <- decode.field(4, decode.string)
  use key <- decode.field(5, decode.string)
  use target <- decode.field(6, decode.string)
  use type_ <- decode.field(7, decode.string)
  use metadata <- decode.field(8, decode.string)
  use payload <- decode.field(9, decode.bit_array)
  use lock_token <- decode.field(10, decode.string)
  decode.success(OutboxMessage(
    id: id,
    source_position: factos.SequencePosition(source_position),
    source_event_id: source_event_id,
    source_context: source_context,
    consumer: consumer,
    key: key,
    target: target,
    type_: type_,
    metadata: metadata_from_text(metadata),
    payload: payload,
    lock_token: lock_token,
  ))
}

fn tags_to_text(tags: List(factos.Tag)) -> String {
  case tags {
    [] -> ""
    [_, ..] ->
      "\n"
      <> { tags |> list.map(factos.tag_value) |> string.join(with: "\n") }
      <> "\n"
  }
}

fn tags_from_text(tags: String) -> List(factos.Tag) {
  case string.is_empty(tags) {
    True -> []
    False ->
      tags
      |> string.split(on: "\n")
      |> list.filter(fn(tag) { !string.is_empty(tag) })
      |> list.map(factos.tag)
  }
}

fn metadata_to_text(metadata: factos.Metadata) -> String {
  metadata
  |> factos.metadata_entries
  |> list.map(fn(entry) { entry.0 <> "=" <> entry.1 })
  |> string.join(with: "\n")
}

fn metadata_from_text(metadata: String) -> factos.Metadata {
  case string.is_empty(metadata) {
    True -> factos.empty_metadata()
    False ->
      metadata
      |> string.split(on: "\n")
      |> list.filter_map(fn(entry) {
        case string.split(entry, on: "=") {
          [key, value] -> Ok(#(key, value))
          _ -> Error(Nil)
        }
      })
      |> factos.metadata
  }
}

pub fn error_to_string(
  error: Error(domain_error),
  domain_error_to_string: fn(domain_error) -> String,
) -> String {
  case error {
    DomainError(error) -> domain_error_to_string(error)
    StoreError(error) -> "store error: " <> query_error_to_string(error)
    AppendConditionFailed(factos.NoAppendCondition) ->
      "append to event failed: No append condition"
    AppendConditionFailed(factos.FailIfEventsMatch(query: _, after: _)) ->
      "append to event failed: Events matched"
    DecodeError(UnknownEvent) -> "unknown event decoded"
    DecodeError(InvalidData) -> "invalid data stored in database"
  }
}

fn query_error_to_string(error: pog.QueryError) -> String {
  case error {
    pog.ConstraintViolated(message:, constraint:, detail:) ->
      "constraint violated: " <> constraint <> ": " <> message <> " " <> detail
    pog.PostgresqlError(code:, name:, message:) ->
      "postgresql error " <> code <> " " <> name <> ": " <> message
    pog.UnexpectedArgumentCount(expected:, got:) ->
      "unexpected argument count: expected "
      <> int.to_string(expected)
      <> ", got "
      <> int.to_string(got)
    pog.UnexpectedArgumentType(expected:, got:) ->
      "unexpected argument type: expected " <> expected <> ", got " <> got
    pog.UnexpectedResultType(errors) ->
      "unexpected result type: " <> int.to_string(list.length(errors))
    pog.QueryTimeout -> "query timeout"
    pog.ConnectionUnavailable -> "connection unavailable"
  }
}

fn nil_constant(_) {
  Nil
}
