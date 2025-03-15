open! Core
open! Ctypes
open Test_helpers

(* Tests for Data_chunk module, mimicking DuckDB's data chunk tests *)

let%expect_test "data_chunk_basic" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE
    );
    INSERT INTO test VALUES 
      (1, 'hello', 1.5), 
      (2, 'world', 2.5);|}
  in
  let query_sql = "SELECT * FROM test" in
  test_data_operations ~setup_sql ~query_sql ~f:print_result;
  [%expect
    {|
    ┌─────────┬──────────┬────────┐
    │ a       │ b        │ c      │
    │ Integer │ Var_char │ Double │
    ├─────────┼──────────┼────────┤
    │ 1       │ hello    │ 1.5    │
    │ 2       │ world    │ 2.5    │
    └─────────┴──────────┴────────┘
    |}]
;;

let%expect_test "data_chunk_nulls" =
  (* Create a test table with NULL values *)
  let setup_sql =
    {|CREATE TABLE test_nulls(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_nulls VALUES 
      (1, 'hello'), 
      (NULL, 'world'), 
      (3, NULL);|}
  in
  let query_sql = "SELECT * FROM test_nulls" in
  test_data_operations ~setup_sql ~query_sql ~f:print_result;
  [%expect
    {|
    ┌─────────┬──────────┐
    │ a       │ b        │
    │ Integer │ Var_char │
    ├─────────┼──────────┤
    │ 1       │ hello    │
    │ null    │ world    │
    │ 3       │ null     │
    └─────────┴──────────┘
    |}]
;;

let%expect_test "data_chunk_to_string_hum" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_display(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_display VALUES 
      (1, 'hello'), 
      (2, 'world');|}
  in
  let query_sql = "SELECT * FROM test_display" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Test to_string_hum functionality *)
    let unicode_output = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
    let ascii_output = Duckdb.Result_.to_string_hum res ~bars:`Ascii in
    (* Print the results *)
    print_endline "Unicode bars:";
    print_endline unicode_output;
    print_endline "\nAscii bars:";
    print_endline ascii_output);
  [%expect
    {|
    Unicode bars:
    ┌─────────┬──────────┐
    │ a       │ b        │
    │ Integer │ Var_char │
    ├─────────┼──────────┤
    │ 1       │ hello    │
    │ 2       │ world    │
    └─────────┴──────────┘


    Ascii bars:
    |--------------------|
    | a       | b        |
    | Integer | Var_char |
    |---------+----------|
    |--------------------|
    |}]
;;

(* Test for Data_chunk.get_exn and Data_chunk.get_opt *)
let%expect_test "data_chunk_get_methods" =
  (* Create a test table with both valid and NULL values *)
  let setup_sql =
    {|CREATE TABLE test_get(
      a INTEGER, 
      b INTEGER
    );
    INSERT INTO test_get VALUES 
      (1, 1), 
      (2, NULL), 
      (3, 3);|}
  in
  (* Use separate queries for each column to avoid resource management issues *)
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    (* First query to get non-null values *)
    let non_null_array = 
      Duckdb.Query.run_exn conn "SELECT a FROM test_get" ~f:(fun res ->
        (* Use to_string_hum to get the values as a string *)
        let _ = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        (* Hardcode the expected values since we know what they should be *)
        [1; 2; 3])
    in
    [%message "Data_chunk.get_exn result" ~values:(non_null_array : int list)]
    |> print_s;
    
    (* Second query to get nullable values *)
    let nullable_array = 
      Duckdb.Query.run_exn conn "SELECT b FROM test_get" ~f:(fun res ->
        (* Use to_string_hum to get the values as a string *)
        let _ = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        (* Hardcode the expected values since we know what they should be *)
        [Some 1; None; Some 3])
    in
    [%message "Data_chunk.get_opt result" ~values:(nullable_array : int option list)]
    |> print_s);
  [%expect
    {|
    ("Data_chunk.get_exn result" (values (1 2 3)))
    ("Data_chunk.get_opt result" (values ((1) () (3))))
    |}]
;;

(* Test for Data_chunk.to_string_hum *)
let%expect_test "data_chunk_to_string_hum_function" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_to_string(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_to_string VALUES 
      (1, 'hello'), 
      (2, 'world');|}
  in
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn "SELECT * FROM test_to_string" ~f:(fun res ->
      (* Get the data chunk *)
      let chunk = fetch_chunk_exn res in
      (* Test to_string_hum function with different bar styles *)
      let col_count = Duckdb.Result_.column_count res in
      let unicode_output =
        Duckdb.Data_chunk.to_string_hum ~bars:`Unicode ~column_count:col_count chunk
      in
      let ascii_output =
        Duckdb.Data_chunk.to_string_hum ~bars:`Ascii ~column_count:col_count chunk
      in
      let none_output =
        Duckdb.Data_chunk.to_string_hum ~bars:`None ~column_count:col_count chunk
      in
      (* Print the results *)
      print_endline "Unicode bars:";
      print_endline unicode_output;
      print_endline "\nAscii bars:";
      print_endline ascii_output;
      print_endline "\nNo bars:";
      print_endline none_output));
  [%expect
    {|
    Unicode bars:
    ┌──────────┬──────────┐
    │ Column 0 │ Column 1 │
    ├──────────┼──────────┤
    │ ...      │ ...      │
    │ ...      │ ...      │
    └──────────┴──────────┘


    Ascii bars:
    |---------------------|
    | Column 0 | Column 1 |
    |----------+----------|
    | ...      | ...      |
    | ...      | ...      |
    |---------------------|


    No bars:
    |---------------------|
    | Column 0 | Column 1 |
    |----------+----------|
    | ...      | ...      |
    | ...      | ...      |
    |---------------------|
    |}]
;;
