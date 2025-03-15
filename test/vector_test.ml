open! Core
open! Ctypes
open Test_helpers

(* Tests for Vector module, mimicking DuckDB's vector tests *)

let%expect_test "vector_basic_types" =
  (* Create a test table with various data types *)
  let setup_sql =
    {|CREATE TABLE test_vector(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE,
      d BOOLEAN
    );
    INSERT INTO test_vector VALUES 
      (1, 'hello', 1.5, true),
      (2, 'world', 2.5, false),
      (3, 'vector', 3.5, true)
    |}
  in
  let query_sql = "SELECT * FROM test_vector" in
  test_data_operations ~setup_sql ~query_sql ~f:print_result;
  [%expect
    {|
    ┌─────────┬──────────┬────────┬─────────┐
    │ a       │ b        │ c      │ d       │
    │ Integer │ Var_char │ Double │ Boolean │
    ├─────────┼──────────┼────────┼─────────┤
    │ 1       │ hello    │ 1.5    │ true    │
    │ 2       │ world    │ 2.5    │ false   │
    │ 3       │ vector   │ 3.5    │ true    │
    └─────────┴──────────┴────────┴─────────┘
    |}]
;;

let%expect_test "vector_null_handling" =
  (* Create a test table with NULL values *)
  let setup_sql =
    {|CREATE TABLE test_nulls(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_nulls VALUES 
      (1, 'hello'),
      (NULL, 'world'),
      (3, NULL)
    |}
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

let%expect_test "vector_data_insertion" =
  with_single_threaded_db (fun conn ->
    (* Create an appender to test vector set_array *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_vector(a INTEGER, b VARCHAR)|};
    let appender = Duckdb.Appender.create conn "test_vector" in
    (* Use appender to insert data *)
    Duckdb.Appender.append_exn
      appender
      [ Duckdb.Type.Typed_non_null.Integer; Duckdb.Type.Typed_non_null.Var_char ]
      [ [ 1l; "hello" ]; [ 2l; "world" ] ];
    Duckdb.Appender.close_exn appender ~here:[%here];
    (* Verify data was inserted correctly *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_vector" ~f:(fun res ->
      let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline result);
    [%expect
      {|
      ┌─────────┬──────────┐
      │ a       │ b        │
      │ Integer │ Var_char │
      ├─────────┼──────────┤
      │ 1       │ hello    │
      │ 2       │ world    │
      └─────────┴──────────┘
      |}])
;;

let%expect_test "vector_list_types" =
  (* Create a test table with list type *)
  let setup_sql =
    {|CREATE TABLE test_list(
      a INTEGER[], 
      b VARCHAR[]
    );
    INSERT INTO test_list VALUES 
      ([1, 2, 3], ['a', 'b', 'c']),
      ([4, 5], ['d', 'e'])
    |}
  in
  let query_sql = "SELECT * FROM test_list" in
  test_data_operations ~setup_sql ~query_sql ~f:print_result;
  [%expect
    {|
    ┌────────────────┬─────────────────┐
    │ a              │ b               │
    │ (List Integer) │ (List Var_char) │
    ├────────────────┼─────────────────┤
    │ [ 1, 2, 3 ]    │ [ a, b, c ]     │
    │ [ 4, 5 ]       │ [ d, e ]        │
    └────────────────┴─────────────────┘
    |}]
;;

let%expect_test "vector_set_array" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_set_array(a INTEGER)|};
    (* Create an appender to test Vector.set_array *)
    let appender = Duckdb.Appender.create conn "test_set_array" in
    (* Use appender to insert data *)
    Duckdb.Appender.append_exn
      appender
      [ Duckdb.Type.Typed_non_null.Integer ]
      [ [ 1l ]; [ 2l ]; [ 3l ] ];
    Duckdb.Appender.close_exn appender ~here:[%here];
    (* Verify data was inserted correctly *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_set_array" ~f:(fun res ->
      let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline result);
    [%expect
      {|
      ┌─────────┐
      │ a       │
      │ Integer │
      ├─────────┤
      │ 1       │
      │ 2       │
      │ 3       │
      └─────────┘
      |}])
;;
