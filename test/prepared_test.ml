open! Core
open! Ctypes

(* Tests for prepared statements, mimicking DuckDB's prepared statement tests *)

let%expect_test "prepared_statement_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test(a INTEGER, b VARCHAR, c DOUBLE)";
      (* Create a prepared statement for insertion *)
      let prepared_insert =
        Duckdb.Query.Prepared.create conn "INSERT INTO test VALUES (?, ?, ?)"
        |> Result.ok_or_failwith
      in
      (* Bind and execute with different values *)
      Duckdb.Query.Prepared.bind
        prepared_insert
        [ Integer, 1; Var_char, "hello"; Double, 1.5 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared_insert;
      Duckdb.Query.Prepared.bind
        prepared_insert
        [ Integer, 2; Var_char, "world"; Double, 2.5 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared_insert;
      (* Create a prepared statement for selection *)
      let prepared_select =
        Duckdb.Query.Prepared.create conn "SELECT * FROM test WHERE a > ? AND c < ?"
        |> Result.ok_or_failwith
      in
      (* Bind and execute with filter values *)
      Duckdb.Query.Prepared.bind prepared_select [ Integer, 0; Double, 2.0 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn prepared_select ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────┬──────────┬──────────┐
        │ a         │ b        │ c        │
        │ Integer   │ Var_char │ Double   │
        ├───────────┼──────────┼──────────┤
        │ 1         │ hello    │ 1.5      │
        └───────────┴──────────┴──────────┘
      |}];
      (* Change the binding and execute again *)
      Duckdb.Query.Prepared.bind prepared_select [ Integer, 1; Double, 3.0 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn prepared_select ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────┬──────────┬──────────┐
        │ a         │ b        │ c        │
        │ Integer   │ Var_char │ Double   │
        ├───────────┼──────────┼──────────┤
        │ 2         │ world    │ 2.5      │
        └───────────┴──────────┴──────────┘
      |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared_insert ~here:[%here];
      Duckdb.Query.Prepared.destroy prepared_select ~here:[%here]))
;;

let%expect_test "prepared_statement_types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table with various types *)
      Duckdb.Query.run_exn'
        conn
        "CREATE TABLE test_types(\n\
        \        a TINYINT, b SMALLINT, c INTEGER, d BIGINT, \n\
        \        e FLOAT, f DOUBLE, g DATE, h VARCHAR)";
      (* Create a prepared statement for insertion *)
      let prepared =
        Duckdb.Query.Prepared.create
          conn
          "INSERT INTO test_types VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        |> Result.ok_or_failwith
      in
      (* Bind and execute with different types *)
      Duckdb.Query.Prepared.bind
        prepared
        [ Tiny_int, 1
        ; Small_int, 2
        ; Integer, 3
        ; Big_int, 4
        ; Float, 1.5
        ; Double, 2.5
        ; Date, Date_.create_exn ~y:2022 ~m:10 ~d:20
        ; Var_char, "hello world"
        ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared;
      (* Query the inserted data *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_types" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌─────────┬───────────┬───────────┬───────────┬──────────┬──────────┬────────────┬──────────┐
        │ a       │ b         │ c         │ d         │ e        │ f        │ g          │ h        │
        │ Tiny_int│ Small_int │ Integer   │ Big_int   │ Float    │ Double   │ Date       │ Var_char │
        ├─────────┼───────────┼───────────┼───────────┼──────────┼──────────┼────────────┼──────────┤
        │ 1       │ 2         │ 3         │ 4         │ 1.5      │ 2.5      │ 2022-10-20 │ hello world│
        └─────────┴───────────┴───────────┴───────────┴──────────┴──────────┴────────────┴──────────┘
      |}];
      (* Test prepared statement with NULL values *)
      let prepared_nulls =
        Duckdb.Query.Prepared.create
          conn
          "INSERT INTO test_types VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        |> Result.ok_or_failwith
      in
      (* Bind with some NULL values *)
      Duckdb.Query.Prepared.bind
        prepared_nulls
        [ Tiny_int, 10
        ; Small_int, 20
        ; Integer, 30
        ; Big_int, 40
        ; Float, Float.nan
        ; (* Will be converted to NULL *)
          Double, Float.infinity
        ; (* Will be converted to NULL in some DBs *)
          Date, Date_.create_exn ~y:2023 ~m:1 ~d:15
        ; Var_char, ""
        ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared_nulls;
      (* Query all data *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_types ORDER BY a" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌─────────┬───────────┬───────────┬───────────┬──────────┬──────────┬────────────┬──────────┐
        │ a       │ b         │ c         │ d         │ e        │ f        │ g          │ h        │
        │ Tiny_int│ Small_int │ Integer   │ Big_int   │ Float    │ Double   │ Date       │ Var_char │
        ├─────────┼───────────┼───────────┼───────────┼──────────┼──────────┼────────────┼──────────┤
        │ 1       │ 2         │ 3         │ 4         │ 1.5      │ 2.5      │ 2022-10-20 │ hello world│
        │ 10      │ 20        │ 30        │ 40        │ nan      │ inf      │ 2023-01-15 │          │
        └─────────┴───────────┴───────────┴───────────┴──────────┴──────────┴────────────┴──────────┘
      |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared ~here:[%here];
      Duckdb.Query.Prepared.destroy prepared_nulls ~here:[%here]))
;;
