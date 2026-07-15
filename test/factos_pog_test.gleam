import factos
import factos/factos_pog
import gleam/bit_array
import gleam/dynamic/decode
import gleam/erlang/application
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleeunit
import pog
import simplifile
import testcontainer
import testcontainer/error as testcontainer_error
import testcontainer_formulas/postgres

pub fn main() -> Nil {
  gleeunit.main()
}

type Command {
  RegisterUser(username: String)
}

type Event {
  UserRegistered(username: String)
}

type Effect {
  SendWelcome(username: String)
}

type State {
  Available
  Taken
}

type DomainError {
  AlreadyTaken
}

type OutboxRowState {
  OutboxRowState(
    status: String,
    attempts: Int,
    lock_token: String,
    locked_until: String,
    available_at: String,
    available: Bool,
    last_error: String,
    delivered_at: String,
    failed_at: String,
    delivered_timestamp_valid: Bool,
    failed_timestamp_valid: Bool,
    delivered_timestamp_absent: Bool,
    failed_timestamp_absent: Bool,
  )
}

type CounterCommand {
  Increment
}

type CounterEvent {
  Incremented(value: Int)
}

type CounterState {
  CounterState(total: Int)
}

pub type Timeout(a) {
  Timeout(time: Int, function: fn() -> a)
}

pub fn compatibility_migration_enforces_uuidv4_identity_contract_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)
      assert_uuidv4_identity_contract(connection)
    })
  Nil
}

pub fn dbmate_migrations_enforce_uuidv4_identity_contract_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema_from_dbmate(connection)
      assert_uuidv4_identity_contract(connection)
    })
  Nil
}

pub fn dispatch_builder_with_one_retry_attempt_persists_events_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let assert Ok(dispatch) =
        factos_pog.new_dispatch(
          connection: connection,
          stream: "user-renata",
          decider: decider(),
          codec: codec(),
        )
        |> factos_pog.with_retry_attempts(attempts: 1)
        |> factos_pog.dispatch(RegisterUser("renata"))

      let assert factos_pog.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert_user_recorded(
        recorded,
        stream: "user-renata",
        revision: 0,
        position: dispatch.append.position,
        username: "renata",
      )
      let reactor = factos.reactor(react: fn(recorded) { [recorded.event] })
      assert factos.react_all(reactor: reactor, events: dispatch.events)
        == [
          UserRegistered("renata"),
        ]

      let assert Ok(loaded) =
        factos_pog.load_stream(
          connection,
          stream: "user-renata",
          decider: decider(),
          codec: codec(),
        )

      assert loaded.state == Taken
      assert loaded.revision == factos.CurrentRevision(0)
      Nil
    })
  Nil
}

pub fn concurrent_dispatch_same_stream_allows_one_empty_state_append_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let messages = process.new_subject()
      start_blocked_dispatch_worker(
        connection,
        messages: messages,
        worker: "first",
        stream: "concurrent-user-renata",
        command: RegisterUser(username: "renata"),
      )
      start_blocked_dispatch_worker(
        connection,
        messages: messages,
        worker: "second",
        stream: "concurrent-user-renata",
        command: RegisterUser(username: "renata"),
      )

      let first_release = receive_dispatch_ready(messages)
      let second_release = receive_dispatch_ready(messages)
      process.send(first_release, Nil)
      let first_result = receive_dispatch_finished(messages)
      process.send(second_release, Nil)
      let second_result = receive_dispatch_finished(messages)
      let results = [first_result, second_result]
      assert list.count(results, where: is_successful_dispatch) == 1
      assert list.count(results, where: is_stale_stream_dispatch) == 1

      let assert Ok(loaded) =
        factos_pog.load_stream(
          connection,
          stream: "concurrent-user-renata",
          decider: decider(),
          codec: codec(),
        )
      assert loaded.state == Taken
      assert list.length(loaded.events) == 1
      let assert [event] = loaded.events
      assert event.event == UserRegistered("renata")
      Nil
    })
  Nil
}

pub fn concurrent_dispatch_with_query_retries_duplicate_username_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let query = username_query("renata")
      let messages = process.new_subject()
      start_blocked_query_dispatch_worker(
        connection,
        messages: messages,
        worker: "first",
        stream: "concurrent-query-renata-1",
        query: query,
        command: RegisterUser(username: "renata"),
      )
      start_blocked_query_dispatch_worker(
        connection,
        messages: messages,
        worker: "second",
        stream: "concurrent-query-renata-2",
        query: query,
        command: RegisterUser(username: "renata"),
      )

      let first_release = receive_dispatch_ready(messages)
      let second_release = receive_dispatch_ready(messages)
      process.send(first_release, Nil)
      let first_result = receive_dispatch_finished(messages)
      process.send(second_release, Nil)
      let second_result = receive_dispatch_finished(messages)
      let results = [first_result, second_result]
      assert list.count(results, where: is_successful_dispatch) == 1
      assert list.count(results, where: is_already_taken_dispatch) == 1

      let assert Ok(context) =
        factos_pog.read_context(
          connection,
          query: query,
          decider: decider(),
          codec: codec(),
        )
      assert context.state == Taken
      assert list.length(context.events) == 1
      let assert [event] = context.events
      assert event.event == UserRegistered("renata")
      Nil
    })
  Nil
}

pub fn dispatch_builder_with_query_filters_before_decoding_unknown_events_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)
      insert_unknown_event(connection)

      let query = username_query("renata")

      let assert Ok(dispatch) =
        factos_pog.new_dispatch(
          connection: connection,
          stream: "user-renata",
          decider: decider(),
          codec: codec(),
        )
        |> factos_pog.with_query(query: query)
        |> factos_pog.dispatch(RegisterUser("renata"))

      let assert factos_pog.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert_user_recorded(
        recorded,
        stream: "user-renata",
        revision: 0,
        position: dispatch.append.position,
        username: "renata",
      )
      let reactor = factos.reactor(react: fn(recorded) { [recorded.event] })
      assert factos.react_all(reactor: reactor, events: dispatch.events)
        == [
          UserRegistered("renata"),
        ]

      let assert Ok(context) =
        factos_pog.read_context(
          connection,
          query: query,
          decider: decider(),
          codec: codec(),
        )

      let assert [event] = context.events
      assert event.event == UserRegistered("renata")
      assert context.state == Taken
      assert context.position != factos.NoPosition
      Nil
    })
  Nil
}

pub fn dispatch_builder_with_query_handles_many_streams_test_() -> Timeout(Nil) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let query =
        factos.query([
          factos.query_item(types: [factos.event_type("Incremented")], tags: [
            factos.tag("counter:load"),
          ]),
        ])

      let assert Ok(dispatch) =
        dispatch_counter_context_many(connection, query, 25)
      let assert factos_pog.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert_counter_recorded(
        recorded,
        stream: "counter-context-1",
        revision: 0,
        position: dispatch.append.position,
        value: 25,
        type_: factos.event_type("Incremented"),
      )
      let reactor = factos.reactor(react: fn(recorded) { [recorded.event] })
      assert factos.react_all(reactor: reactor, events: dispatch.events)
        == [
          Incremented(25),
        ]

      let assert Ok(context) =
        factos_pog.read_context(
          connection,
          query: query,
          decider: counter_decider(),
          codec: counter_codec(),
        )

      assert context.state == CounterState(25)
      assert list.length(context.events) == 25
      Nil
    })
  Nil
}

pub fn read_events_after_returns_global_position_slice_test_() -> Timeout(Nil) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let query =
        factos.query([
          factos.query_item(types: [factos.event_type("UserRegistered")], tags: [
            factos.tag("username:renata"),
          ]),
        ])
      let assert Ok(first_dispatch) =
        factos_pog.new_dispatch(
          connection: connection,
          stream: "user-renata",
          decider: decider(),
          codec: codec(),
        )
        |> factos_pog.with_query(query: query)
        |> factos_pog.dispatch(RegisterUser("renata"))

      let assert Ok(second_dispatch) =
        factos_pog.new_dispatch(
          connection: connection,
          stream: "user-lucy",
          decider: decider(),
          codec: codec(),
        )
        |> factos_pog.with_query(query: username_query("lucy"))
        |> factos_pog.dispatch(RegisterUser("lucy"))

      let assert Ok(events) =
        factos_pog.read_events_after(
          connection,
          query: factos.AllEvents,
          after: first_dispatch.append.position,
          limit: 10,
          codec: codec(),
        )

      let assert [event] = events
      assert event.event == UserRegistered("lucy")
      assert event.position == second_dispatch.append.position
      Nil
    })
  Nil
}

pub fn outbox_lease_returns_persisted_token_and_ack_is_single_use_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let #(dispatch, message) = dispatch_welcome_and_lease(connection)
      let assert factos.SequencePosition(source_position) =
        dispatch.append.position

      assert message.source_position == factos.SequencePosition(source_position)
      assert message.source_context == "user-renata"
      assert message.consumer == "wallets"
      assert message.key == "welcome:renata"
      assert message.target == "ledgers"
      assert message.type_ == "SendWelcome"
      assert factos.metadata_get(message.metadata, factos.correlation_id)
        == Ok("corr-renata")
      assert bit_array.to_string(message.payload) == Ok("renata")
      assert message.lock_token != ""

      let leased = read_outbox_state(connection, message.id)
      assert leased.status == "pending"
      assert leased.attempts == 1
      assert leased.lock_token == message.lock_token
      assert leased.locked_until != ""
      assert leased.delivered_timestamp_absent
      assert leased.failed_timestamp_absent

      let assert Ok(factos_pog.Finalized) =
        factos_pog.ack_outbox(
          connection,
          id: message.id,
          lock_token: message.lock_token,
        )

      let delivered = read_outbox_state(connection, message.id)
      assert delivered.status == "delivered"
      assert delivered.attempts == 1
      assert delivered.lock_token == ""
      assert delivered.locked_until == ""
      assert delivered.delivered_timestamp_valid
      assert delivered.failed_timestamp_absent

      let assert Ok(factos_pog.StaleLease) =
        factos_pog.ack_outbox(
          connection,
          id: message.id,
          lock_token: message.lock_token,
        )
      assert read_outbox_state(connection, message.id) == delivered

      let assert Ok([]) = lease_outbox(connection)
      Nil
    })
  Nil
}

pub fn outbox_nack_releases_matching_lease_for_delayed_retry_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)
      let #(_, first_lease) = dispatch_welcome_and_lease(connection)

      let assert Ok(factos_pog.Finalized) =
        factos_pog.nack_outbox(
          connection,
          id: first_lease.id,
          lock_token: first_lease.lock_token,
          error: "ledger temporarily unavailable",
          retry_after_milliseconds: 60_000,
        )

      let released = read_outbox_state(connection, first_lease.id)
      assert released.status == "pending"
      assert released.attempts == 1
      assert released.lock_token == ""
      assert released.locked_until == ""
      assert !released.available
      assert released.last_error == "ledger temporarily unavailable"
      assert released.delivered_timestamp_absent
      assert released.failed_timestamp_absent
      let assert Ok([]) = lease_outbox(connection)

      make_outbox_available(connection, first_lease.id)
      let assert Ok([second_lease]) = lease_outbox(connection)
      assert second_lease.id == first_lease.id
      assert second_lease.lock_token != first_lease.lock_token

      let retried = read_outbox_state(connection, second_lease.id)
      assert retried.status == "pending"
      assert retried.attempts == 2
      assert retried.lock_token == second_lease.lock_token
      assert retried.locked_until != ""
      assert retried.delivered_timestamp_absent
      assert retried.failed_timestamp_absent
      Nil
    })
  Nil
}

pub fn outbox_wrong_and_expired_tokens_cannot_finalize_current_lease_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)
      let #(_, first_lease) = dispatch_welcome_and_lease(connection)
      let wrong_token = "00000000-0000-4000-8000-000000000000"
      let leased = read_outbox_state(connection, first_lease.id)

      assert_all_finalizers_report_stale(
        connection,
        first_lease.id,
        wrong_token,
      )
      assert read_outbox_state(connection, first_lease.id) == leased

      expire_outbox_lease(connection, first_lease.id)
      let expired = read_outbox_state(connection, first_lease.id)
      let assert Ok(factos_pog.StaleLease) =
        factos_pog.ack_outbox(
          connection,
          id: first_lease.id,
          lock_token: first_lease.lock_token,
        )
      assert read_outbox_state(connection, first_lease.id) == expired

      let assert Ok([second_lease]) = lease_outbox(connection)
      assert second_lease.id == first_lease.id
      assert second_lease.lock_token != first_lease.lock_token

      let reacquired = read_outbox_state(connection, second_lease.id)
      assert reacquired.status == "pending"
      assert reacquired.attempts == 2
      assert reacquired.lock_token == second_lease.lock_token
      assert reacquired.locked_until != ""
      assert reacquired.delivered_timestamp_absent
      assert reacquired.failed_timestamp_absent

      assert_all_finalizers_report_stale(
        connection,
        second_lease.id,
        first_lease.lock_token,
      )
      assert read_outbox_state(connection, second_lease.id) == reacquired

      let assert Ok(factos_pog.Finalized) =
        factos_pog.ack_outbox(
          connection,
          id: second_lease.id,
          lock_token: second_lease.lock_token,
        )
      Nil
    })
  Nil
}

pub fn outbox_fail_dead_letters_matching_lease_and_is_terminal_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)
      let #(_, message) = dispatch_welcome_and_lease(connection)

      let assert Ok(factos_pog.Finalized) =
        factos_pog.fail_outbox(
          connection,
          id: message.id,
          lock_token: message.lock_token,
          error: "permanent ledger rejection",
        )

      let dead_lettered = read_outbox_state(connection, message.id)
      assert dead_lettered.status == "dead_lettered"
      assert dead_lettered.attempts == 1
      assert dead_lettered.lock_token == ""
      assert dead_lettered.locked_until == ""
      assert dead_lettered.last_error == "permanent ledger rejection"
      assert dead_lettered.failed_timestamp_valid
      assert dead_lettered.delivered_timestamp_absent

      assert_all_finalizers_report_stale(
        connection,
        message.id,
        message.lock_token,
      )
      assert read_outbox_state(connection, message.id) == dead_lettered
      let assert Ok([]) = lease_outbox(connection)
      Nil
    })
  Nil
}

fn dispatch_welcome_and_lease(
  connection: pog.Connection,
) -> #(factos_pog.Dispatch(Event), factos_pog.OutboxMessage) {
  let assert Ok(dispatch) =
    factos_pog.new_dispatch(
      connection: connection,
      stream: "user-renata",
      decider: decider(),
      codec: codec(),
    )
    |> factos_pog.with_query(query: username_query("renata"))
    |> factos_pog.with_reactor(
      reactor: welcome_reactor(),
      codec: effect_codec(),
    )
    |> factos_pog.dispatch(RegisterUser("renata"))
  let assert Ok([message]) = lease_outbox(connection)
  #(dispatch, message)
}

fn lease_outbox(
  connection: pog.Connection,
) -> Result(List(factos_pog.OutboxMessage), factos_pog.Error(Nil)) {
  factos_pog.lease_outbox(
    connection,
    consumer: "wallets",
    target: "ledgers",
    limit: 10,
    lease_for_milliseconds: 30_000,
  )
}

fn assert_all_finalizers_report_stale(
  connection: pog.Connection,
  id: Int,
  lock_token: String,
) -> Nil {
  let assert Ok(factos_pog.StaleLease) =
    factos_pog.ack_outbox(connection, id: id, lock_token: lock_token)
  let assert Ok(factos_pog.StaleLease) =
    factos_pog.nack_outbox(
      connection,
      id: id,
      lock_token: lock_token,
      error: "must not replace the current error",
      retry_after_milliseconds: 60_000,
    )
  let assert Ok(factos_pog.StaleLease) =
    factos_pog.fail_outbox(
      connection,
      id: id,
      lock_token: lock_token,
      error: "must not dead-letter",
    )
  Nil
}

fn read_outbox_state(connection: pog.Connection, id: Int) -> OutboxRowState {
  let assert Ok(returned) =
    pog.query(
      "
      select
        status,
        attempts,
        coalesce(lock_token, ''),
        coalesce(locked_until::text, ''),
        available_at::text,
        available_at <= now(),
        coalesce(last_error, ''),
        coalesce(delivered_at::text, ''),
        coalesce(failed_at::text, ''),
        coalesce(delivered_at >= created_at, false),
        coalesce(failed_at >= created_at, false),
        delivered_at is null,
        failed_at is null
      from factos_outbox
      where id = $1
      ",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning(outbox_row_state_decoder())
    |> pog.execute(on: connection)
  let assert [state] = returned.rows
  state
}

fn outbox_row_state_decoder() -> decode.Decoder(OutboxRowState) {
  use status <- decode.field(0, decode.string)
  use attempts <- decode.field(1, decode.int)
  use lock_token <- decode.field(2, decode.string)
  use locked_until <- decode.field(3, decode.string)
  use available_at <- decode.field(4, decode.string)
  use available <- decode.field(5, decode.bool)
  use last_error <- decode.field(6, decode.string)
  use delivered_at <- decode.field(7, decode.string)
  use failed_at <- decode.field(8, decode.string)
  use delivered_timestamp_valid <- decode.field(9, decode.bool)
  use failed_timestamp_valid <- decode.field(10, decode.bool)
  use delivered_timestamp_absent <- decode.field(11, decode.bool)
  use failed_timestamp_absent <- decode.field(12, decode.bool)
  decode.success(OutboxRowState(
    status: status,
    attempts: attempts,
    lock_token: lock_token,
    locked_until: locked_until,
    available_at: available_at,
    available: available,
    last_error: last_error,
    delivered_at: delivered_at,
    failed_at: failed_at,
    delivered_timestamp_valid: delivered_timestamp_valid,
    failed_timestamp_valid: failed_timestamp_valid,
    delivered_timestamp_absent: delivered_timestamp_absent,
    failed_timestamp_absent: failed_timestamp_absent,
  ))
}

fn expire_outbox_lease(connection: pog.Connection, id: Int) -> Nil {
  let assert Ok(_) =
    pog.query(
      "update factos_outbox set locked_until = now() - interval '1 second' where id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  Nil
}

fn make_outbox_available(connection: pog.Connection, id: Int) -> Nil {
  let assert Ok(_) =
    pog.query("update factos_outbox set available_at = now() where id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  Nil
}

type DispatchMessage {
  DispatchReady(worker: String, release: process.Subject(Nil))
  DispatchFinished(
    worker: String,
    result: Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)),
  )
}

fn start_blocked_dispatch_worker(
  connection: pog.Connection,
  messages messages: process.Subject(DispatchMessage),
  worker worker: String,
  stream stream_name: String,
  command command: Command,
) -> process.Pid {
  process.spawn(fn() {
    let result =
      factos_pog.new_dispatch(
        connection: connection,
        stream: stream_name,
        decider: blocking_decider(messages, worker: worker),
        codec: worker_codec(worker),
      )
      |> factos_pog.dispatch(command)
    process.send(messages, DispatchFinished(worker: worker, result: result))
  })
}

fn start_blocked_query_dispatch_worker(
  connection: pog.Connection,
  messages messages: process.Subject(DispatchMessage),
  worker worker: String,
  stream stream_name: String,
  query query: factos.Query,
  command command: Command,
) -> process.Pid {
  process.spawn(fn() {
    let result =
      factos_pog.new_dispatch(
        connection: connection,
        stream: stream_name,
        decider: blocking_decider(messages, worker: worker),
        codec: worker_codec(worker),
      )
      |> factos_pog.with_query(query: query)
      |> factos_pog.dispatch(command)
    process.send(messages, DispatchFinished(worker: worker, result: result))
  })
}

fn blocking_decider(
  messages: process.Subject(DispatchMessage),
  worker worker: String,
) -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(
    initial: Available,
    decide: fn(state, command) {
      case state, command {
        Available, RegisterUser(username) -> {
          let release = process.new_subject()
          process.send(
            messages,
            DispatchReady(worker: worker, release: release),
          )
          let assert Ok(Nil) = process.receive(release, within: 10_000)
          Ok([UserRegistered(username)])
        }
        Taken, RegisterUser(_) -> Error(AlreadyTaken)
      }
    },
    evolve: evolve,
  )
}

fn receive_dispatch_ready(
  messages: process.Subject(DispatchMessage),
) -> process.Subject(Nil) {
  let assert Ok(message) = process.receive(messages, within: 10_000)
  let assert DispatchReady(worker: _, release: release) = message
  release
}

fn receive_dispatch_finished(
  messages: process.Subject(DispatchMessage),
) -> Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)) {
  let assert Ok(message) = process.receive(messages, within: 10_000)
  case message {
    DispatchFinished(worker: _, result:) -> result
    DispatchReady(worker: _, release:) -> {
      process.send(release, Nil)
      receive_dispatch_finished(messages)
    }
  }
}

fn is_successful_dispatch(
  result: Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)),
) -> Bool {
  case result {
    Ok(_) -> True
    Error(_) -> False
  }
}

fn is_stale_stream_dispatch(
  result: Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)),
) -> Bool {
  case result {
    Error(factos_pog.AppendConditionFailed(factos.NoAppendCondition)) -> True
    Error(factos_pog.DomainError(AlreadyTaken)) -> True
    _ -> False
  }
}

fn is_already_taken_dispatch(
  result: Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)),
) -> Bool {
  case result {
    Error(factos_pog.DomainError(AlreadyTaken)) -> True
    _ -> False
  }
}

fn with_test_connection(
  body: fn(pog.Connection) -> Nil,
) -> Result(Nil, testcontainer_error.Error) {
  use postgres_container <- testcontainer.with_formula(
    postgres.new() |> postgres.formula(),
  )
  let #(pool_pid, connection) = start_test_connection(postgres_container)
  body(connection)
  process.send_exit(pool_pid)
  process.sleep(100)
  Ok(Nil)
}

fn start_test_connection(
  postgres_container: postgres.PostgresContainer,
) -> #(process.Pid, pog.Connection) {
  let postgres.PostgresContainer(host:, port:, database:, username:, ..) =
    postgres_container
  let pool_name = process.new_name("factos_pog_test")
  let config =
    pog.default_config(pool_name)
    |> pog.host(host)
    |> pog.port(port)
    |> pog.database(database)
    |> pog.user(username)
    |> pog.password(Some("postgres"))
    |> pog.ssl(pog.SslDisabled)

  let assert Ok(actor.Started(pid:, ..)) = pog.start(config)
  process.sleep(100)
  #(pid, pog.named_connection(pool_name))
}

fn reset_schema(connection: pog.Connection) -> Nil {
  drop_schema(connection)
  execute_migration_file(connection)
}

fn reset_schema_from_dbmate(connection: pog.Connection) -> Nil {
  drop_schema(connection)
  let assert Ok(priv_directory) = application.priv_directory("factos_pog")
  execute_dbmate_up(
    connection,
    priv_directory <> "/dbmate/20260703000100_factos_pog_event_store.sql",
  )
  execute_dbmate_up(
    connection,
    priv_directory <> "/dbmate/20260704000100_factos_pog_outbox.sql",
  )
  execute_dbmate_up(
    connection,
    priv_directory
      <> "/dbmate/20260714000100_factos_pog_outbox_delivery_safety.sql",
  )
}

fn drop_schema(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query("drop table if exists factos_outbox")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_event_tags")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_events")
    |> pog.execute(on: connection)
  Nil
}

fn execute_migration_file(connection: pog.Connection) -> Nil {
  let assert Ok(priv_directory) = application.priv_directory("factos_pog")
  let assert Ok(sql) = simplifile.read(priv_directory <> "/migrations.sql")
  execute_sql(connection, sql)
}

fn execute_dbmate_up(connection: pog.Connection, path: String) -> Nil {
  let assert Ok(sql) = simplifile.read(path)
  let assert [up, ..] = string.split(sql, "-- migrate:down")
  execute_sql(connection, up)
}

fn execute_sql(connection: pog.Connection, sql: String) -> Nil {
  sql
  |> string.split(";")
  |> list.each(fn(statement) {
    case string.trim(statement) {
      "" -> Nil
      statement -> {
        let assert Ok(_) = pog.query(statement) |> pog.execute(on: connection)
        Nil
      }
    }
  })
}

fn assert_uuidv4_identity_contract(connection: pog.Connection) -> Nil {
  let valid_id = "b3b12f1d-6d85-4f1f-9c2a-94766a34f011"
  assert insert_event_succeeds(connection, valid_id, "valid-stream")

  [
    #("customer-registered", "semantic-stream"),
    #("event-b3b12f1d-6d85-4f1f-9c2a-94766a34f012", "prefixed-stream"),
    #("b3b12f1d-6d85-5f1f-9c2a-94766a34f013", "wrong-version-stream"),
    #("not-a-uuid", "malformed-stream"),
  ]
  |> list.each(fn(case_) {
    let #(invalid_id, stream_name) = case_
    assert !insert_event_succeeds(connection, invalid_id, stream_name)
  })

  assert !insert_event_succeeds(connection, valid_id, "different-stream")
  assert insert_outbox_succeeds(
    connection,
    source_event_id: valid_id,
    effect_key: "valid-effect",
  )

  [
    #("customer-registered", "semantic-effect"),
    #("event-b3b12f1d-6d85-4f1f-9c2a-94766a34f012", "prefixed-effect"),
    #("b3b12f1d-6d85-4f1f-7c2a-94766a34f013", "wrong-variant-effect"),
    #("not-a-uuid", "malformed-effect"),
  ]
  |> list.each(fn(case_) {
    let #(invalid_id, effect_key) = case_
    assert !insert_outbox_succeeds(
      connection,
      source_event_id: invalid_id,
      effect_key: effect_key,
    )
  })
}

fn insert_event_succeeds(
  connection: pog.Connection,
  id: String,
  stream_name: String,
) -> Bool {
  case
    pog.query(
      "
      insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
      values ($1, $2, 0, 'ContractChecked', 1, '', '', '\\x00')
      ",
    )
    |> pog.parameter(pog.text(id))
    |> pog.parameter(pog.text(stream_name))
    |> pog.execute(on: connection)
  {
    Ok(_) -> True
    Error(_) -> False
  }
}

fn insert_outbox_succeeds(
  connection: pog.Connection,
  source_event_id source_event_id: String,
  effect_key effect_key: String,
) -> Bool {
  case
    pog.query(
      "
      insert into factos_outbox (
        source_position, source_event_id, source_context, consumer,
        effect_key, target, type, metadata, payload
      )
      values (
        (select position from factos_events where id = $1),
        $2, 'valid-stream', 'contract-test', $3, 'sink', 'Effect', '', '\\x00'
      )
      ",
    )
    |> pog.parameter(pog.text("b3b12f1d-6d85-4f1f-9c2a-94766a34f011"))
    |> pog.parameter(pog.text(source_event_id))
    |> pog.parameter(pog.text(effect_key))
    |> pog.execute(on: connection)
  {
    Ok(_) -> True
    Error(_) -> False
  }
}

fn insert_unknown_event(connection: pog.Connection) -> Nil {
  let tag = "username:intruder"
  let assert Ok(_) =
    pog.query(
      "
      with inserted as (
        insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        returning position
      )
      insert into factos_event_tags(position, tag)
      select position, $9 from inserted
      ",
    )
    |> pog.parameter(pog.text("b3b12f1d-6d85-4f1f-9c2a-94766a34f004"))
    |> pog.parameter(pog.text("unknown-stream"))
    |> pog.parameter(pog.int(0))
    |> pog.parameter(pog.text("UnknownEventType"))
    |> pog.parameter(pog.int(1))
    |> pog.parameter(pog.text("\n" <> tag <> "\n"))
    |> pog.parameter(pog.text(""))
    |> pog.parameter(pog.bytea(bit_array.from_string("unknown")))
    |> pog.parameter(pog.text(tag))
    |> pog.execute(on: connection)
  Nil
}

fn username_query(username: String) -> factos.Query {
  factos.query([
    factos.query_item(types: [factos.event_type("UserRegistered")], tags: [
      factos.tag("username:" <> username),
    ]),
  ])
}

fn assert_user_recorded(
  recorded: factos.Recorded(Event),
  stream stream_name: String,
  revision revision: Int,
  position position: factos.SequencePosition,
  username username: String,
) -> Nil {
  assert recorded.id == user_event_id(username)
  assert recorded.stream == stream_name
  assert recorded.revision == revision
  assert recorded.position == position
  assert recorded.type_ == factos.event_type("UserRegistered")
  assert recorded.version == 1
  assert recorded.tags == [factos.tag("username:" <> username)]
  assert recorded.metadata == factos.empty_metadata()
  assert recorded.event == UserRegistered(username)
}

fn decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(initial: Available, decide:, evolve:)
}

fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  case state, command {
    Available, RegisterUser(username) -> Ok([UserRegistered(username)])
    Taken, RegisterUser(_) -> Error(AlreadyTaken)
  }
}

fn evolve(_state: State, _event: Event) -> State {
  Taken
}

fn codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode:, decode:)
}

fn encode(event: Event) -> factos_pog.Proposed(Event) {
  proposed_event(event, user_event_id(event.username))
}

fn worker_codec(worker: String) -> factos_pog.EventCodec(Event) {
  factos_pog.codec(
    encode: fn(event) { proposed_event(event, worker_event_id(worker)) },
    decode: decode,
  )
}

fn proposed_event(event: Event, id: String) -> factos_pog.Proposed(Event) {
  factos_pog.Proposed(
    id: id,
    event: event,
    type_: factos.event_type("UserRegistered"),
    version: 1,
    tags: [factos.tag("username:" <> event.username)],
    metadata: factos.empty_metadata(),
    data: bit_array.from_string(event.username),
  )
}

fn worker_event_id(worker: String) -> String {
  case worker {
    "first" -> "b3b12f1d-6d85-4f1f-9c2a-94766a34f005"
    _ -> "b3b12f1d-6d85-4f1f-9c2a-94766a34f006"
  }
}

fn user_event_id(username: String) -> String {
  case username {
    "renata" -> "b3b12f1d-6d85-4f1f-9c2a-94766a34f001"
    "lucy" -> "b3b12f1d-6d85-4f1f-9c2a-94766a34f002"
    _ -> "b3b12f1d-6d85-4f1f-9c2a-94766a34f003"
  }
}

fn decode(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(Event), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "UserRegistered" -> {
      use username <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: UserRegistered(username),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}

fn welcome_reactor() -> factos.Reactor(Event, Effect) {
  factos.reactor(react: fn(recorded) {
    case recorded.event {
      UserRegistered(username) -> [SendWelcome(username)]
    }
  })
}

fn effect_codec() -> factos_pog.EffectCodec(Effect) {
  factos_pog.effect_codec(encode: encode_effect)
}

fn encode_effect(effect: Effect) -> factos_pog.ProposedEffect(Effect) {
  case effect {
    SendWelcome(username) ->
      factos_pog.ProposedEffect(
        effect: effect,
        consumer: "wallets",
        key: "welcome:" <> username,
        target: "ledgers",
        type_: "SendWelcome",
        metadata: factos.metadata([
          #(factos.correlation_id, "corr-" <> username),
        ]),
        payload: bit_array.from_string(username),
      )
  }
}

fn dispatch_counter_context_many(
  connection: pog.Connection,
  query: factos.Query,
  remaining: Int,
) -> Result(factos_pog.Dispatch(CounterEvent), factos_pog.Error(Nil)) {
  case remaining {
    0 ->
      factos_pog.new_dispatch(
        connection: connection,
        stream: "counter-context-0",
        decider: counter_decider(),
        codec: counter_codec(),
      )
      |> factos_pog.with_query(query: query)
      |> factos_pog.dispatch(Increment)
    _ -> {
      let stream_name = "counter-context-" <> int.to_string(remaining)
      let result =
        factos_pog.new_dispatch(
          connection: connection,
          stream: stream_name,
          decider: counter_decider(),
          codec: counter_codec(),
        )
        |> factos_pog.with_query(query: query)
        |> factos_pog.dispatch(Increment)
      case remaining, result {
        1, _ -> result
        _, Ok(_) ->
          dispatch_counter_context_many(connection, query, remaining - 1)
        _, Error(error) -> Error(error)
      }
    }
  }
}

fn counter_decider() -> factos.Decider(
  CounterCommand,
  CounterState,
  CounterEvent,
  Nil,
) {
  factos.decider(
    initial: CounterState(0),
    decide: counter_decide,
    evolve: counter_evolve,
  )
}

fn counter_decide(
  state: CounterState,
  command: CounterCommand,
) -> Result(List(CounterEvent), Nil) {
  let CounterState(total) = state
  case command {
    Increment -> Ok([Incremented(total + 1)])
  }
}

fn counter_evolve(state: CounterState, event: CounterEvent) -> CounterState {
  let CounterState(total) = state
  case event {
    Incremented(_) -> CounterState(total + 1)
  }
}

fn counter_codec() -> factos_pog.EventCodec(CounterEvent) {
  factos_pog.codec(encode: encode_counter_event, decode: decode_counter_event)
}

fn encode_counter_event(
  event: CounterEvent,
) -> factos_pog.Proposed(CounterEvent) {
  case event {
    Incremented(value) ->
      factos_pog.Proposed(
        id: counter_event_id(value),
        event: event,
        type_: factos.event_type("Incremented"),
        version: 1,
        tags: [factos.tag("counter:load")],
        metadata: factos.empty_metadata(),
        data: bit_array.from_string(int.to_string(value)),
      )
  }
}

fn counter_event_id(value: Int) -> String {
  "b3b12f1d-6d85-4f1f-9c2a-" <> string.pad_start(int.to_string(value), 12, "0")
}

fn decode_counter_event(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(CounterEvent), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "Incremented" -> {
      use text <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      use value <- result.try(
        int.parse(text)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: Incremented(value),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}

fn assert_counter_recorded(
  recorded: factos.Recorded(CounterEvent),
  stream stream_name: String,
  revision revision: Int,
  position position: factos.SequencePosition,
  value value: Int,
  type_ type_: factos.EventType,
) -> Nil {
  assert recorded.id == counter_event_id(value)
  assert recorded.stream == stream_name
  assert recorded.revision == revision
  assert recorded.position == position
  assert recorded.type_ == type_
  assert recorded.version == 1
  assert recorded.tags == [factos.tag("counter:load")]
  assert recorded.metadata == factos.empty_metadata()
  assert recorded.event == Incremented(value)
}
