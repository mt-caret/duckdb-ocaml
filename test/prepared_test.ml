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
        [%message "Prepared statement result" ~result:(result : string)] |> print_s);
      [%expect
        {|
        ("Prepared statement result"
         (result
          "┌───────────┬──────────┬──────────┐\n│ a         │ b        │ c        │\n│ Integer   │ Var_char │ Double   │\n├───────────┼──────────┼──────────┤\n│ 1         │ hello    │ 1.5      │\n└───────────┴──────────┴──────────┘"))
        |}];
      (* Change the binding and execute again *)
      Duckdb.Query.Prepared.bind prepared_select [ Integer, 1l; Double, 3.0 ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn prepared_select ~f:(fun res ->
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        [%message "Prepared statement result (updated)" ~result:(result : string)]
        |> print_s);
      [%expect
        {|
        ("Prepared statement result (updated)"
         (result
          "┌───────────┬──────────┬──────────┐\n│ a         │ b        │ c        │\n│ Integer   │ Var_char │ Double   │\n├───────────┼──────────┼──────────┤\n│ 2         │ world    │ 2.5      │\n└───────────┴──────────┴──────────┘"))
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
        {|CREATE TABLE test_types(
          a TINYINT, b SMALLINT, c INTEGER, d BIGINT, 
          e FLOAT, f DOUBLE, g DATE, h VARCHAR)|};
      (* Create a prepared statement for insertion *)
      let prepared =
        Duckdb.Query.Prepared.create
          conn
          {|INSERT INTO test_types VALUES (?, ?, ?, ?, ?, ?, ?, ?)|}
        |> Result.ok_or_failwith
      in
      (* Create a date value *)
      let date =
        let date = make Duckdb_stubs.Date.t in
        setf date Duckdb_stubs.Date.days 19000l;
        date
      in
      (* Bind and execute with different types *)
      Duckdb.Query.Prepared.bind
        prepared
        [ Tiny_int, 1
        ; Small_int, 2
        ; Integer, 3l
        ; Big_int, 4L
        ; Float, 1.5
        ; Double, 2.5
        ; Date, date
        ; Var_char, "hello world"
        ]
      |> Result.ok_or_failwith;
      Duckdb.Query.Prepared.run_exn' prepared;
      (* Query the inserted data *)
      Duckdb.Query.run_exn conn {|SELECT * FROM test_types|} ~f:(fun res ->
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        [%message "Prepared statement with various types" ~result:(result : string)]
        |> print_s);
      [%expect
        {|
        ("Prepared statement with various types"
         (result
          "┌─────────┬───────────┬───────────┬───────────┬──────────┬──────────┬────────────┬──────────┐\n│ a       │ b         │ c         │ d         │ e        │ f        │ g          │ h        │\n│ Tiny_int│ Small_int │ Integer   │ Big_int   │ Float    │ Double   │ Date       │ Var_char │\n├─────────┼───────────┼───────────┼───────────┼──────────┼──────────┼────────────┼──────────┤\n│ 1       │ 2         │ 3         │ 4         │ 1.5      │ 2.5      │ 2022-01-05 │ hello world│\n└─────────┴───────────┴───────────┴───────────┴──────────┴──────────┴────────────┴──────────┘"))
        |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared ~here:[%here]))
;;
