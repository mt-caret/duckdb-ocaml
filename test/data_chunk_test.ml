open! Core
open! Ctypes
open Duckdb

(* Tests for Data_chunk module, mimicking DuckDB's data chunk tests *)

(* Helper function to set up single-threaded mode for all tests *)
let with_single_threaded_db f =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      f conn))
;;

(* Helper function to test data_chunk operations on a simple table *)
let test_data_chunk_operations ~setup_sql ~query_sql ~f =
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f)
;;

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
  test_data_chunk_operations ~setup_sql ~query_sql ~f:(fun res ->
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
  test_data_chunk_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Print the result using to_string_hum *)
    let result_str = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
    print_endline result_str);
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

let%expect_test "data_chunk_column_count" =
  (* Create a test table with multiple columns *)
  let setup_sql =
    {|CREATE TABLE test_columns(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE, 
      d BOOLEAN, 
      e DATE
    );
    INSERT INTO test_columns VALUES 
      (1, 'hello', 1.5, true, '2023-01-01');|}
  in
  let query_sql = "SELECT * FROM test_columns" in
  test_data_chunk_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Fetch a data chunk *)
    let data_chunk =
      Duckdb.Result_.fetch res ~f:(fun opt ->
        Option.value_exn opt ~message:"Expected data chunk but got None")
    in
    (* Test column_count function *)
    let count = Duckdb.Data_chunk.column_count data_chunk in
    (* Free the data chunk *)
    Duckdb.Data_chunk.free data_chunk ~here:[%here];
    (* Print column count *)
    [%message "Data chunk column count" ~count:(count : int)] |> print_s);
  [%expect {| ("Data chunk column count" (count 5)) |}]
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
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f:(fun res ->
      (* Test to_string_hum functionality *)
      let unicode_output = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      let ascii_output = Duckdb.Result_.to_string_hum res ~bars:`Ascii in
      (* Print the results *)
      print_endline "Unicode bars:";
      print_endline unicode_output;
      print_endline "\nAscii bars:";
      print_endline ascii_output));
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
    +---------+----------+
    | a       | b        |
    | Integer | Var_char |
    +---------+----------+
    | 1       | hello    |
    | 2       | world    |
    +---------+----------+
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
  let query_sql = "SELECT * FROM test_get" in
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f:(fun res ->
      (* Get the data chunk *)
      let chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Test get_exn on column without NULLs *)
      let non_null_array = Duckdb.Data_chunk.get_exn chunk Integer 0 in
      [%message "Data_chunk.get_exn result" ~values:(non_null_array : int32 array)]
      |> print_s;
      (* Test get_opt on column with NULLs *)
      let nullable_array = Duckdb.Data_chunk.get_opt chunk Integer 1 in
      [%message "Data_chunk.get_opt result" ~values:(nullable_array : int32 option array)]
      |> print_s;
      (* Free the data chunk *)
      Duckdb.Data_chunk.free chunk ~here:[%here]));
  [%expect
    {|
    ("Data_chunk.get_exn result" (values (1l 2l 3l)))
    ("Data_chunk.get_opt result" (values ((Some 1l) None (Some 3l))))
    |}]
;;

(* Test for Data_chunk.column_count *)
let%expect_test "data_chunk_column_count_function" =
  (* Create a test table with multiple columns *)
  let setup_sql =
    {|CREATE TABLE test_col_count(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE
    );
    INSERT INTO test_col_count VALUES (1, 'hello', 1.5);|}
  in
  let query_sql = "SELECT * FROM test_col_count" in
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f:(fun res ->
      (* Get the data chunk *)
      let chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Test column_count function *)
      let count = Duckdb.Data_chunk.column_count chunk in
      [%message "Data_chunk.column_count result" ~count:(count : int)] |> print_s;
      (* Free the data chunk *)
      Duckdb.Data_chunk.free chunk ~here:[%here]));
  [%expect {| ("Data_chunk.column_count result" (count 3)) |}]
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
  let query_sql = "SELECT * FROM test_to_string" in
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f:(fun res ->
      (* Get the data chunk *)
      let chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Test to_string_hum function with different bar styles *)
      let unicode_output = Duckdb.Data_chunk.to_string_hum chunk ~bars:`Unicode in
      let ascii_output = Duckdb.Data_chunk.to_string_hum chunk ~bars:`Ascii in
      let none_output = Duckdb.Data_chunk.to_string_hum chunk ~bars:`None in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free chunk ~here:[%here];
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
    ┌─────────┬──────────┐
    │ a       │ b        │
    │ Integer │ Var_char │
    ├─────────┼──────────┤
    │ 1       │ hello    │
    │ 2       │ world    │
    └─────────┴──────────┘

    Ascii bars:
    +---------+----------+
    | a       | b        |
    | Integer | Var_char |
    +---------+----------+
    | 1       | hello    |
    | 2       | world    |
    +---------+----------+

    No bars:
    a       b        
    Integer Var_char 
                     
    1       hello    
    2       world    
    |}]
;;
