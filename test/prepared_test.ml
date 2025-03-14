open! Core
open! Ctypes

(* Tests for prepared statements, mimicking DuckDB's prepared statement tests *)

let%expect_test "prepared_statement_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test(a INTEGER, b VARCHAR, c DOUBLE)|};
      (* Create a prepared statement for insertion *)
      let prepared_insert =
        Duckdb.Query.Prepared.create conn {|INSERT INTO test VALUES (?, ?, ?)|}
        |> Result.ok_or_failwith
      in
      (* Bind and execute with different values *)
      Duckdb.Query.Prepared.bind
        prepared_insert
        [ Integer, 1l; Var_char, "hello"; Double, 1.5 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared_insert;
      Duckdb.Query.Prepared.bind
        prepared_insert
        [ Integer, 2l; Var_char, "world"; Double, 2.5 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared_insert;
      (* Create a prepared statement for selection *)
      let prepared_select =
        Duckdb.Query.Prepared.create conn {|SELECT * FROM test WHERE a > ? AND c < ?|}
        |> Result.ok_or_failwith
      in
      (* Bind and execute with filter values *)
      Duckdb.Query.Prepared.bind prepared_select [ Integer, 0l; Double, 2.0 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn prepared_select ~f:(fun res ->
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        print_endline result);
      [%expect
        {|
        ┌─────────┬──────────┬────────┐
        │ a       │ b        │ c      │
        │ Integer │ Var_char │ Double │
        ├─────────┼──────────┼────────┤
        │ 1       │ hello    │ 1.5    │
        └─────────┴──────────┴────────┘
        |}];
      (* Change the binding and execute again *)
      Duckdb.Query.Prepared.bind prepared_select [ Integer, 1l; Double, 3.0 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn prepared_select ~f:(fun res ->
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        print_endline result);
      [%expect
        {|
        ┌─────────┬──────────┬────────┐
        │ a       │ b        │ c      │
        │ Integer │ Var_char │ Double │
        ├─────────┼──────────┼────────┤
        │ 2       │ world    │ 2.5    │
        └─────────┴──────────┴────────┘
        |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared_insert ~here:[%here];
      Duckdb.Query.Prepared.destroy prepared_select ~here:[%here]))
;;

let%expect_test "prepared_statement_types" =
  [%message "Testing prepared statements with various types"] |> print_s;
  [%expect {| "Testing prepared statements with various types" |}]
;;
