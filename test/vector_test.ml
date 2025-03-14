open! Core
open! Ctypes

(* Tests for Vector module, mimicking DuckDB's vector tests *)

let%expect_test "vector_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
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
        (* Get the first vector (integer column) *)
        let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 in
        [%message "Integer vector values" ~values:(int_array : int32 array)] |> print_s;
        [%expect {| ("Integer vector values" (values (1 2))) |}];
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;

let%expect_test "vector_nulls" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
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
        (* Test get_opt for integer column with NULL *)
        let int_array = Duckdb.Data_chunk.get_opt data_chunk Integer 0 in
        [%message "Integer vector with nulls" ~values:(int_array : int32 option array)]
        |> print_s;
        [%expect {| ("Integer vector with nulls" (values ((1) () (3)))) |}];
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;

let%expect_test "vector_set_array" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
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
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        [%message "Appended data" ~result:(result : string)] |> print_s);
      [%expect
        {|
        ("Appended data"
         (result
          "┌─────────┬──────────┐\n│ a       │ b        │\n│ Integer │ Var_char │\n├─────────┼──────────┤\n│ 1       │ hello    │\n│ 2       │ world    │\n└─────────┴──────────┘"))
        |}]))
;;
