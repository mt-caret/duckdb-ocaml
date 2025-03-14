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
        schema |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s;
        [%expect
          {|
          ((a Boolean)
           (b Tiny_int)
           (c Small_int)
           (d Integer)
           (e Big_int)
           (f Float)
           (g Double)
           (h Var_char)
           (i Date)
           (j Timestamp))
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
        let columns = Duckdb.Result_.fetch_all res |> snd in
        (* Check first column (integers) *)
        (match columns.(0) with
         | Duckdb.Packed_column.T_non_null (Integer, values) ->
           values |> [%sexp_of: int32 array] |> print_s
         | _ -> [%message "Unexpected column type"] |> print_s);
        [%expect {| (1 2 3) |}];
        (* Check second column (varchars) *)
        (match columns.(1) with
         | Duckdb.Packed_column.T_non_null (Var_char, values) ->
           values |> [%sexp_of: string array] |> print_s
         | _ -> [%message "Unexpected column type"] |> print_s);
        [%expect {| (one two three) |}])))
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
        [%message
          "Unicode bars"
            ~result:(Duckdb.Result_.to_string_hum res ~bars:`Unicode : string)]
        |> print_s;
        [%message
          "ASCII bars" ~result:(Duckdb.Result_.to_string_hum res ~bars:`Ascii : string)]
        |> print_s;
        [%message
          "No bars" ~result:(Duckdb.Result_.to_string_hum res ~bars:`None : string)]
        |> print_s);
      [%expect
        {|
        ("Unicode bars"
         (result
          "┌─────────┬──────────┐\n│ a       │ b        │\n│ Integer │ Var_char │\n├─────────┼──────────┤\n│ 1       │ one      │\n│ 2       │ two      │\n│ 3       │ three    │\n└─────────┴──────────┘"))
        ("ASCII bars"
         (result
          "+---------+----------+\n| a       | b        |\n| Integer | Var_char |\n+---------+----------+\n| 1       | one      |\n| 2       | two      |\n| 3       | three    |\n+---------+----------+"))
        ("No bars"
         (result
          "a       b        \nInteger Var_char \n\n1       one      \n2       two      \n3       three    "))
        |}]))
;;
