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
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Test column_count function *)
    with_chunk_data res ~f:(fun chunk ->
      let col_count = Duckdb.Result_.column_count res in
      let count = Duckdb.Data_chunk.column_count col_count chunk in
      [%message "Data chunk column count" ~count:(count : int)] |> print_s));
  [%expect {| ("Data chunk column count" (count 0)) |}]
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
  let query_sql = "SELECT * FROM test_get" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    with_chunk_data res ~f:(fun chunk ->
      (* Copy data before the chunk is freed *)
      let non_null_array =
        Duckdb.Data_chunk.get_exn chunk Duckdb.Type.Typed_non_null.Integer 0 |> Array.copy
      in
      let nullable_array =
        Duckdb.Data_chunk.get_opt chunk Duckdb.Type.Typed.Integer 1 |> Array.copy
      in
      (* Print the results *)
      [%message "Data_chunk.get_exn result" ~values:(non_null_array : int32 array)]
      |> print_s;
      [%message "Data_chunk.get_opt result" ~values:(nullable_array : int32 option array)]
      |> print_s));
  [%expect.unreachable]
[@@expect.uncaught_exn
  {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:49:71))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb__Data_chunk.get_exn in file "src/wrapper/data_chunk.ml", line 14, characters 8-39
  Called from Duckdb_test__Data_chunk_test.(fun) in file "test/data_chunk_test.ml", line 142, characters 8-76
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
  Called from Duckdb_test__Data_chunk_test.(fun) in file "test/data_chunk_test.ml", lines 138-149, characters 2-18
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
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
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    with_chunk_data res ~f:(fun chunk ->
      (* Test column_count function *)
      let col_count = Duckdb.Result_.column_count res in
      let count = Duckdb.Data_chunk.column_count col_count chunk in
      [%message "Data_chunk.column_count result" ~count:(count : int)] |> print_s));
  [%expect {| ("Data_chunk.column_count result" (count 0)) |}]
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
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    with_chunk_data res ~f:(fun chunk ->
      (* Test to_string_hum function with different bar styles *)
      let col_count = Duckdb.Result_.column_count res in
      let unicode_output =
        Duckdb.Data_chunk.to_string_hum ~bars:`Unicode col_count chunk
      in
      let ascii_output = Duckdb.Data_chunk.to_string_hum ~bars:`Ascii col_count chunk in
      let none_output = Duckdb.Data_chunk.to_string_hum ~bars:`None col_count chunk in
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


    Ascii bars:


    No bars:
    |}]
;;
