import factos
import factos/factos_pog
import gleam/bit_array
import gleam/erlang/application
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleamy/bench
import pog
import simplifile
import testcontainer
import testcontainer/error as testcontainer_error
import testcontainer_formulas/postgres

const workers = 8

const operations_per_worker = 5

pub fn main() -> Nil {
  let assert Ok(Nil) = run()
  Nil
}

fn run() -> Result(Nil, testcontainer_error.Error) {
  io.println("factos_pog row-lock benchmark")
  io.println(
    "Each benchmark iteration dispatches "
    <> int.to_string(workers * operations_per_worker)
    <> " commands.",
  )

  use postgres_container <- testcontainer.with_formula(
    postgres.new() |> postgres.formula(),
  )
  let #(pool_pid, connection) = start_connection(postgres_container)
  let input = BenchmarkInput(connection: connection)

  bench.run(
    [bench.Input("postgres", input)],
    [
      bench.SetupFunction("sequential dispatch", setup_sequential),
      bench.SetupFunction("concurrent dispatch", setup_concurrent),
    ],
    [bench.Duration(400), bench.Warmup(50), bench.Decimals(2)],
  )
  |> bench.table([bench.IPS, bench.Min, bench.Mean, bench.P(99)])
  |> io.println

  process.send_exit(pool_pid)
  process.sleep(100)
  Ok(Nil)
}

type BenchmarkInput {
  BenchmarkInput(connection: pog.Connection)
}

type Command {
  Increment
}

type Event {
  Incremented(value: Int)
}

type State {
  Counter(total: Int)
}

type WorkerMessage {
  WorkerDone(worker: Int, result: Result(Nil, factos_pog.Error(Nil)))
}

fn setup_sequential(input: BenchmarkInput) -> fn(BenchmarkInput) -> Nil {
  reset_schema(input.connection)
  fn(input: BenchmarkInput) {
    run_sequential(input.connection, workers * operations_per_worker)
  }
}

fn setup_concurrent(input: BenchmarkInput) -> fn(BenchmarkInput) -> Nil {
  reset_schema(input.connection)
  fn(input: BenchmarkInput) {
    run_concurrent(input.connection, workers, operations_per_worker)
  }
}

fn start_connection(
  postgres_container: postgres.PostgresContainer,
) -> #(process.Pid, pog.Connection) {
  let postgres.PostgresContainer(host:, port:, database:, username:, ..) =
    postgres_container
  let pool_name = process.new_name("factos_pog_dev")
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
  let assert Ok(_) =
    pog.query("drop table if exists factos_outbox")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_event_tags")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_events")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_lock")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_locks")
    |> pog.execute(on: connection)
  execute_migration_file(connection)
}

fn execute_migration_file(connection: pog.Connection) -> Nil {
  let assert Ok(priv_directory) = application.priv_directory("factos_pog")
  let assert Ok(sql) = simplifile.read(priv_directory <> "/migrations.sql")

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

fn run_sequential(connection: pog.Connection, remaining: Int) -> Nil {
  case remaining <= 0 {
    True -> Nil
    False -> {
      let assert Ok(_) = dispatch_once(connection, "sequential")
      run_sequential(connection, remaining - 1)
    }
  }
}

fn run_concurrent(
  connection: pog.Connection,
  worker_count: Int,
  operations_count: Int,
) -> Nil {
  let subject = process.new_subject()
  spawn_workers(connection, subject, worker_count, operations_count)
  wait_for_workers(subject, worker_count)
}

fn spawn_workers(
  connection: pog.Connection,
  subject: process.Subject(WorkerMessage),
  worker_count: Int,
  operations_count: Int,
) -> Nil {
  case worker_count <= 0 {
    True -> Nil
    False -> {
      let worker = worker_count
      process.spawn(fn() {
        let result = run_worker(connection, worker, operations_count)
        process.send(subject, WorkerDone(worker: worker, result: result))
      })
      spawn_workers(connection, subject, worker_count - 1, operations_count)
    }
  }
}

fn wait_for_workers(
  subject: process.Subject(WorkerMessage),
  remaining: Int,
) -> Nil {
  case remaining <= 0 {
    True -> Nil
    False -> {
      let assert Ok(message) = process.receive(subject, within: 120_000)
      case message {
        WorkerDone(worker: _, result: Ok(Nil)) ->
          wait_for_workers(subject, remaining - 1)
        WorkerDone(worker: _, result: Error(_)) ->
          panic as "benchmark worker failed"
      }
    }
  }
}

fn run_worker(
  connection: pog.Connection,
  worker: Int,
  remaining: Int,
) -> Result(Nil, factos_pog.Error(Nil)) {
  case remaining <= 0 {
    True -> Ok(Nil)
    False -> {
      use _ <- result.try(dispatch_once(
        connection,
        "concurrent-" <> int.to_string(worker),
      ))
      run_worker(connection, worker, remaining - 1)
    }
  }
}

fn dispatch_once(
  connection: pog.Connection,
  stream_name: String,
) -> Result(factos_pog.Dispatch(Event), factos_pog.Error(Nil)) {
  factos_pog.dispatch(
    connection,
    stream: stream_name,
    decider: decider(),
    codec: codec(),
    command: Increment,
  )
}

fn decider() -> factos.Decider(Command, State, Event, Nil) {
  factos.decider(initial: Counter(0), decide:, evolve:)
}

fn decide(state: State, command: Command) -> Result(List(Event), Nil) {
  let Counter(total) = state
  case command {
    Increment -> Ok([Incremented(total + 1)])
  }
}

fn evolve(state: State, event: Event) -> State {
  let Counter(total) = state
  case event {
    Incremented(_) -> Counter(total + 1)
  }
}

fn codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode:, decode:)
}

fn encode(event: Event) -> factos_pog.Proposed(Event) {
  let Incremented(value) = event
  factos_pog.Proposed(
    id: "event-" <> int.to_string(value),
    event: event,
    type_: factos.event_type("Incremented"),
    version: 1,
    tags: [factos.tag("benchmark")],
    metadata: factos.empty_metadata(),
    data: bit_array.from_string(int.to_string(value)),
  )
}

fn decode(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(Event), factos_pog.DecodeError) {
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
