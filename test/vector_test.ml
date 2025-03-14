open! Core
open! Ctypes

(* Tests for Vector module, mimicking DuckDB's vector tests *)

let%expect_test "vector_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      (* Create a test table *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test(a INTEGER, b VARCHAR, c DOUBLE)|};
      Duckdb.Query.run_exn'
        conn
        {|INSERT INTO test VALUES (1, 'hello', 1.5), (2, 'world', 2.5)|};
      (* Run a query that returns a result *)
      Duckdb.Query.run_exn conn "SELECT * FROM test" ~f:(fun res ->
        (* Get data chunk and vector *)
        let data_chunk =
          Duckdb.Result_.fetch res ~f:(function
            | None -> failwith "Expected data chunk but got None"
            | Some chunk -> chunk)
        in
        let vector =
          Duckdb_stubs.duckdb_data_chunk_get_vector
            !@(Duckdb.Data_chunk.Private.to_ptr data_chunk |> Ctypes.addr)
            (Unsigned.UInt64.of_int 0)
        in
        (* Test to_array_exn *)
        let int_array = Duckdb.Vector.to_array_exn vector Integer ~length:2 in
        int_array |> [%sexp_of: int32 array] |> print_s;
        [%expect {| (1 2) |}];
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;

let%expect_test "vector_nulls" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      (* Create a test table with NULL values *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test_nulls(a INTEGER, b VARCHAR)|};
      Duckdb.Query.run_exn'
        conn
        {|INSERT INTO test_nulls VALUES (1, 'hello'), (NULL, 'world'), (3, NULL)|};
      (* Run a query that returns a result with NULLs *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_nulls" ~f:(fun res ->
        (* Get data chunk and vector *)
        let data_chunk =
          Duckdb.Result_.fetch res ~f:(function
            | None -> failwith "Expected data chunk but got None"
            | Some chunk -> chunk)
        in
        let vector =
          Duckdb_stubs.duckdb_data_chunk_get_vector
            !@(Duckdb.Data_chunk.Private.to_ptr data_chunk |> Ctypes.addr)
            (Unsigned.UInt64.of_int 0)
        in
        (* Test to_option_array with NULLs *)
        let int_array = Duckdb.Vector.to_option_array vector Integer ~length:3 in
        int_array |> [%sexp_of: int32 option array] |> print_s;
        [%expect {| ((1) () (3)) |}];
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;

let%expect_test "vector_set_array" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      (* Create an appender to test vector set_array *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test_vector(a INTEGER, b VARCHAR)|};
      let appender = Duckdb.Appender.create conn "test_vector" in
      (* Use appender to insert data *)
      Duckdb.Appender.append_exn
        appender
        [ Integer; Var_char ]
        [ [ 1l; "hello" ]; [ 2l; "world" ] ];
      Duckdb.Appender.close_exn appender ~here:[%here];
      (* Verify data was inserted correctly *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_vector" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌─────────┬──────────┐
        │ a       │ b        │
        │ Integer │ Var_char │
        ├─────────┼──────────┤
        │ 1       │ hello    │
        │ 2       │ world    │
        └─────────┴──────────┘
        |}]))
;;
