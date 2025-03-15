open! Core
open! Ctypes
open Duckdb

(* Tests for Result_ module, mimicking DuckDB's result tests *)

(* Helper function to set up single-threaded mode for all tests *)
let with_single_threaded_db f =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      f conn))
;;

let%expect_test "result_basic" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn'
      conn
      {|CREATE TABLE test_result(
        a INTEGER, 
        b VARCHAR, 
        c DOUBLE
      )|};
    Duckdb.Query.run_exn'
      conn
      {|INSERT INTO test_result VALUES 
        (1, 'hello', 1.5), 
        (2, 'world', 2.5)|};
    (* Query the data to test result operations *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_result" ~f:(fun res ->
      (* Print the result using to_string_hum *)
      let result_str = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline result_str);
    [%expect
      {|
      ┌─────────┬──────────┬────────┐
      │ a       │ b        │ c      │
      │ Integer │ Var_char │ Double │
      ├─────────┼──────────┼────────┤
      │ 1       │ hello    │ 1.5    │
      │ 2       │ world    │ 2.5    │
      └─────────┴──────────┴────────┘
      |}])
;;

let%expect_test "result_column_count" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with multiple columns *)
    Duckdb.Query.run_exn'
      conn
      {|CREATE TABLE test_columns(
        a INTEGER, 
        b VARCHAR, 
        c DOUBLE, 
        d BOOLEAN, 
        e DATE
      )|};
    Duckdb.Query.run_exn'
      conn
      {|INSERT INTO test_columns VALUES 
        (1, 'hello', 1.5, true, '2023-01-01')|};
    (* Query the data to test column_count *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_columns" ~f:(fun res ->
      (* Get the column count *)
      let count = Duckdb.Result_.column_count res in
      [%message "Result column count" ~count:(count : int)] |> print_s);
    [%expect {| ("Result column count" (count 5)) |}])
;;

let%expect_test "result_column_name" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with named columns *)
    Duckdb.Query.run_exn'
      conn
      {|CREATE TABLE test_names(
        id INTEGER, 
        name VARCHAR, 
        value DOUBLE
      )|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_names VALUES (1, 'test', 1.5)|};
    (* Query the data to test column_name *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_names" ~f:(fun res ->
      (* Get the column names *)
      let col_names =
        List.init (Duckdb.Result_.column_count res) ~f:(fun i ->
          Duckdb.Result_.column_name res i)
      in
      [%message "Result column names" ~names:(col_names : string list)] |> print_s);
    [%expect {| ("Result column names" (names (id name value))) |}])
;;

let%expect_test "result_column_type" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with different types *)
    Duckdb.Query.run_exn'
      conn
      {|CREATE TABLE test_types(
        a INTEGER, 
        b VARCHAR, 
        c DOUBLE, 
        d BOOLEAN
      )|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_types VALUES (1, 'test', 1.5, true)|};
    (* Query the data to test column_type *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_types" ~f:(fun res ->
      (* Get the column types *)
      let col_types =
        List.init (Duckdb.Result_.column_count res) ~f:(fun i ->
          Duckdb.Result_.column_type res i |> Type.to_string)
      in
      [%message "Result column types" ~types:(col_types : string list)] |> print_s);
    [%expect {| ("Result column types" (types (INTEGER VARCHAR DOUBLE BOOLEAN))) |}])
;;

let%expect_test "result_fetch" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_fetch(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_fetch VALUES (1), (2), (3)|};
    (* Query the data to test fetch *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_fetch" ~f:(fun res ->
      (* Use fetch to get a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get the data from the chunk *)
      let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 |> Array.copy in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* Print the array *)
      [%message "Fetched data" ~values:(int_array : int32 array)] |> print_s);
    [%expect {| ("Fetched data" (values (1l 2l 3l))) |}])
;;

let%expect_test "result_fetch_all" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_fetch_all(a INTEGER)|};
    Duckdb.Query.run_exn'
      conn
      {|INSERT INTO test_fetch_all VALUES (1), (2), (3), (4), (5)|};
    (* Query the data to test fetch_all *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_fetch_all" ~f:(fun res ->
      (* Use fetch_all to get all data *)
      let columns = Duckdb.Result_.fetch_all res in
      (* Get the first column *)
      let int_column = List.hd_exn columns in
      (* Print the column *)
      [%message "Fetched all data" ~column:(int_column : Packed_column.t)] |> print_s);
    [%expect {| ("Fetched all data" (column <Packed_column>)) |}])
;;

let%expect_test "result_consumption_behavior" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_consume(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_consume VALUES (1), (2), (3)|};
    (* Run a query and demonstrate that to_string_hum consumes results *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_consume" ~f:(fun res ->
      (* First call to to_string_hum *)
      let result1 = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline "First call to to_string_hum:";
      print_endline result1;
      (* Second call to to_string_hum on the same result *)
      let result2 = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline "\nSecond call to to_string_hum:";
      print_endline result2));
  [%expect
    {|
      First call to to_string_hum:
      ┌─────────┐
      │ a       │
      │ Integer │
      ├─────────┤
      │ 1       │
      │ 2       │
      │ 3       │
      └─────────┘

      Second call to to_string_hum:
      
      |}]
;;
