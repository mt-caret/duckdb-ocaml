open! Core
open! Ctypes

(* Tests for Result_ module *)

let%expect_test "result_schema" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table with various types *)
      Duckdb.Query.run_exn'
        conn
        {|CREATE TABLE test_types(
          a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT,
          f FLOAT, g DOUBLE, h VARCHAR, i DATE, j TIMESTAMP
        )|};
      (* Query the schema *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_types" ~f:(fun res ->
        (* Test schema function *)
        let schema = Duckdb.Result_.schema res in
        [%message "Result schema" ~schema:(schema : (string * Duckdb.Type.t) array)]
        |> print_s;
        [%expect
          {|
          ("Result schema"
           (schema
            ((a Boolean) (b Tiny_int) (c Small_int) (d Integer) (e Big_int) (f Float)
             (g Double) (h Var_char) (i Date) (j Timestamp))))
          |}])))
;;

let%expect_test "result_fetch_all" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create and populate a test table *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test(a INTEGER, b VARCHAR)|};
      Duckdb.Query.run_exn'
        conn
        {|INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')|};
      (* Test fetch_all *)
      Duckdb.Query.run_exn conn "SELECT * FROM test ORDER BY a" ~f:(fun res ->
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        print_endline result);
      [%expect
        {|
        ┌─────────┬──────────┐
        │ a       │ b        │
        │ Integer │ Var_char │
        ├─────────┼──────────┤
        │ 1       │ one      │
        │ 2       │ two      │
        │ 3       │ three    │
        └─────────┴──────────┘
        |}]))
;;

let%expect_test "result_to_string_hum" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create and populate a test table *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test(a INTEGER, b VARCHAR)|};
      Duckdb.Query.run_exn'
        conn
        {|INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')|};
      (* Test to_string_hum with different bar styles *)
      Duckdb.Query.run_exn conn "SELECT * FROM test ORDER BY a" ~f:(fun res ->
        print_endline "Unicode bars:";
        print_endline (Duckdb.Result_.to_string_hum res ~bars:`Unicode);
        print_endline "ASCII bars:";
        print_endline (Duckdb.Result_.to_string_hum res ~bars:`Ascii);
        print_endline "ASCII bars again:";
        print_endline (Duckdb.Result_.to_string_hum res ~bars:`Ascii));
      [%expect
        {|
        Unicode bars:
        ┌─────────┬──────────┐
        │ a       │ b        │
        │ Integer │ Var_char │
        ├─────────┼──────────┤
        │ 1       │ one      │
        │ 2       │ two      │
        │ 3       │ three    │
        └─────────┴──────────┘

        ASCII bars:
        |--------------------|
        | a       | b        |
        | Integer | Var_char |
        |---------+----------|
        |--------------------|

        ASCII bars again:
        |--------------------|
        | a       | b        |
        | Integer | Var_char |
        |---------+----------|
        |--------------------|
        |}]))
;;
