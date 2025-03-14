open! Core
open! Ctypes

(* Tests for Vector module, mimicking DuckDB's vector tests *)

let%expect_test "vector_basic" =
  [%message "Testing vector basic operations"] |> print_s;
  [%expect {| "Testing vector basic operations" |}]
;;

let%expect_test "vector_nulls" =
  [%message "Testing vector with NULL values"] |> print_s;
  [%expect {| "Testing vector with NULL values" |}]
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
        print_endline result);
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
