open! Core
open! Ctypes
open Test_helpers

(* Tests for Result_ module, mimicking DuckDB's result tests *)

let%expect_test "result_basic" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_result(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE
    );
    INSERT INTO test_result VALUES 
      (1, 'hello', 1.5), 
      (2, 'world', 2.5)|}
  in
  let query_sql = "SELECT * FROM test_result" in
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

let%expect_test "result_column_count" =
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
      (1, 'hello', 1.5, true, '2023-01-01')|}
  in
  let query_sql = "SELECT * FROM test_columns" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Get the column count *)
    let count = Duckdb.Result_.column_count res in
    [%message "Result column count" ~count:(count : int)] |> print_s);
  [%expect {| ("Result column count" (count 5)) |}]
;;

let%expect_test "result_column_name" =
  (* Create a test table with named columns *)
  let setup_sql =
    {|CREATE TABLE test_names(
      id INTEGER, 
      name VARCHAR, 
      value DOUBLE
    );
    INSERT INTO test_names VALUES (1, 'test', 1.5)|}
  in
  let query_sql = "SELECT * FROM test_names" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Get the column names *)
    let col_names =
      List.init (Duckdb.Result_.column_count res) ~f:(fun i ->
        Duckdb.Result_.column_name res i)
    in
    [%message "Result column names" ~names:(col_names : string list)] |> print_s);
  [%expect {| ("Result column names" (names (id name value))) |}]
;;

let%expect_test "result_column_type" =
  (* Create a test table with different types *)
  let setup_sql =
    {|CREATE TABLE test_types(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE, 
      d BOOLEAN
    );
    INSERT INTO test_types VALUES (1, 'test', 1.5, true)|}
  in
  let query_sql = "SELECT * FROM test_types" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Get the column types *)
    let col_types =
      List.init (Duckdb.Result_.column_count res) ~f:(fun i ->
        Duckdb.Result_.column_type res i |> Duckdb.Type.sexp_of_t |> Sexp.to_string)
    in
    [%message "Result column types" ~types:(col_types : string list)] |> print_s);
  [%expect {| ("Result column types" (types (Integer Var_char Double Boolean))) |}]
;;

let%expect_test "result_fetch" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_fetch(a INTEGER);
    INSERT INTO test_fetch VALUES (1), (2), (3)|}
  in
  let query_sql = "SELECT * FROM test_fetch" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    with_chunk_data res ~f:(fun data_chunk ->
      (* Get the data from the chunk *)
      let int_array =
        Duckdb.Data_chunk.get_exn data_chunk Duckdb.Type.Typed_non_null.Integer 0
        |> Array.copy
      in
      (* Print the array *)
      [%message "Fetched data" ~values:(int_array : int32 array)] |> print_s));
  [%expect.unreachable]
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:49:67))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb__Data_chunk.get_exn in file "src/wrapper/data_chunk.ml", line 14, characters 8-39
  Called from Duckdb_test__Result_test.(fun) in file "test/result_test.ml", line 108, characters 8-81
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb_test__Result_test.(fun) in file "test/result_test.ml", lines 104-112, characters 2-78
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;

let%expect_test "result_fetch_all" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_fetch_all(a INTEGER);
    INSERT INTO test_fetch_all VALUES (1), (2), (3), (4), (5)|}
  in
  let query_sql = "SELECT * FROM test_fetch_all" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Use fetch_all to get all data *)
    let row_count, columns = Duckdb.Result_.fetch_all res in
    (* Print information about the fetched data *)
    [%message
      "Fetched all data"
        ~row_count:(row_count : int)
        ~column_count:(Array.length columns : int)]
    |> print_s);
  [%expect {| ("Fetched all data" (row_count 5) (column_count 1)) |}]
;;

let%expect_test "result_consumption_behavior" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_consume(a INTEGER);
    INSERT INTO test_consume VALUES (1), (2), (3)|}
  in
  let query_sql = "SELECT * FROM test_consume" in
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f:(fun res ->
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
    ┌─────────┐
    │ a       │
    │ Integer │
    ├┬┬┬┬┬┬┬┬┬┤
    └┴┴┴┴┴┴┴┴┴┘
    |}]
;;
