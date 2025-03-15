open! Core
open! Ctypes
open Duckdb

(* Tests for Vector module, mimicking DuckDB's vector tests *)

(* Helper function to set up single-threaded mode for all tests *)
let with_single_threaded_db f =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      f conn))
;;

let%expect_test "vector_basic_types" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with various data types *)
    Duckdb.Query.run_exn'
      conn
      {|CREATE TABLE test_vector(
        a INTEGER, 
        b VARCHAR, 
        c DOUBLE,
        d BOOLEAN
      )|};
    (* Insert test data *)
    Duckdb.Query.run_exn'
      conn
      {|INSERT INTO test_vector VALUES 
        (1, 'hello', 1.5, true),
        (2, 'world', 2.5, false),
        (3, 'vector', 3.5, true)
      |};
    (* Query the data to test vector operations *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_vector" ~f:(fun res ->
      (* Print the result using to_string_hum *)
      let result_str = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline result_str);
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
      |}])
;;

let%expect_test "vector_null_handling" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with NULL values *)
    Duckdb.Query.run_exn'
      conn
      {|CREATE TABLE test_nulls(
        a INTEGER, 
        b VARCHAR
      )|};
    (* Insert test data with NULLs *)
    Duckdb.Query.run_exn'
      conn
      {|INSERT INTO test_nulls VALUES 
        (1, 'hello'),
        (NULL, 'world'),
        (3, NULL)
      |};
    (* Query the data to test vector operations with NULLs *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_nulls" ~f:(fun res ->
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
      |}])
;;

let%expect_test "vector_data_insertion" =
  with_single_threaded_db (fun conn ->
    (* Create an appender to test vector set_array *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_vector(a INTEGER, b VARCHAR)|};
    let appender = Duckdb.Appender.create conn "test_vector" in
    (* Use appender to insert data *)
    Duckdb.Appender.append_exn
      appender
      [ Integer; Var_char ]
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

let%expect_test "vector_validity_checking" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with all valid values *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_valid(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_valid VALUES (1), (2), (3)|};
    (* Query the data to test validity *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_valid" ~f:(fun res ->
      (* Fetch a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get column data directly using Data_chunk.get_exn and copy before the chunk is freed *)
      let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 |> Array.copy in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* All values should be valid *)
      [%message "All values in array" ~values:(int_array : int32 array)] |> print_s);
    [%expect.unreachable];
    (* Now test with NULL values *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_invalid(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_invalid VALUES (1), (NULL), (3)|};
    (* Query the data to test validity with NULLs *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_invalid" ~f:(fun res ->
      (* Fetch a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get column data with nulls and copy before the chunk is freed *)
      let int_array = Duckdb.Data_chunk.get_opt data_chunk Integer 0 |> Array.copy in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* Verify that we have null values in the array *)
      let has_nulls = Array.exists int_array ~f:Option.is_none in
      [%message "Array contains nulls" ~has_nulls:(has_nulls : bool)] |> print_s);
    [%expect.unreachable])
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
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", line 128, characters 22-68
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", lines 121-132, characters 4-84
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;

let%expect_test "vector_list_types" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with list type *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_list(a INTEGER[], b VARCHAR[])|};
    Duckdb.Query.run_exn'
      conn
      {|INSERT INTO test_list VALUES 
        ([1, 2, 3], ['a', 'b', 'c']),
        ([4, 5], ['d', 'e'])
      |};
    (* Query the data to test vector operations with list types *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_list" ~f:(fun res ->
      (* Print the result using to_string_hum *)
      let result_str = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
      print_endline result_str);
    [%expect
      {|
      ┌────────────────┬─────────────────┐
      │ a              │ b               │
      │ (List Integer) │ (List Var_char) │
      ├────────────────┼─────────────────┤
      │ [ 1, 2, 3 ]    │ [ a, b, c ]     │
      │ [ 4, 5 ]       │ [ d, e ]        │
      └────────────────┴─────────────────┘
      |}])
;;

(* Test for Vector.to_array_exn *)
let%expect_test "vector_to_array_exn" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_to_array(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_to_array VALUES (1), (2), (3)|};
    (* Query the data to test Vector.to_array_exn *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_to_array" ~f:(fun res ->
      (* Fetch a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get the vector from the data chunk *)
      let vector =
        Duckdb_stubs.duckdb_data_chunk_get_vector
          !@(Resource.get_exn (Duckdb.Data_chunk.Private.to_ptr data_chunk))
          (Unsigned.UInt64.of_int 0)
      in
      (* Use Vector.to_array_exn to get the data *)
      let int_array = Vector.to_array_exn vector Type.Typed_non_null.Integer ~length:3 in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* Print the array *)
      [%message "Vector.to_array_exn result" ~values:(int_array : int32 array)] |> print_s);
    [%expect.unreachable])
[@@expect.uncaught_exn
  {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:49:71))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", line 222, characters 12-76
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", lines 213-230, characters 4-91
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;

(* Test for Vector.to_option_array *)
let%expect_test "vector_to_option_array" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with NULL values *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_to_option(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_to_option VALUES (1), (NULL), (3)|};
    (* Query the data to test Vector.to_option_array *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_to_option" ~f:(fun res ->
      (* Fetch a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get the vector from the data chunk *)
      let vector =
        Duckdb_stubs.duckdb_data_chunk_get_vector
          !@(Resource.get_exn (Duckdb.Data_chunk.Private.to_ptr data_chunk))
          (Unsigned.UInt64.of_int 0)
      in
      (* Use Vector.to_option_array to get the data *)
      let int_array = Vector.to_option_array vector Type.Typed.Integer ~length:3 in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* Print the array *)
      [%message "Vector.to_option_array result" ~values:(int_array : int32 option array)]
      |> print_s);
    [%expect.unreachable])
[@@expect.uncaught_exn
  {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:49:71))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", line 274, characters 12-76
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", lines 265-283, characters 4-17
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;

(* Test for Vector.set_array *)
let%expect_test "vector_set_array" =
  with_single_threaded_db (fun conn ->
    (* Create a test table *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_set_array(a INTEGER)|};
    (* Create an appender to test Vector.set_array *)
    let appender = Duckdb.Appender.create conn "test_set_array" in
    (* Use appender to insert data *)
    Duckdb.Appender.append_exn appender [ Integer ] [ [ 1l ]; [ 2l ]; [ 3l ] ];
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

(* Test for Vector.assert_all_valid *)
let%expect_test "vector_assert_all_valid" =
  with_single_threaded_db (fun conn ->
    (* Create a test table with all valid values *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_assert_valid(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_assert_valid VALUES (1), (2), (3)|};
    (* Query the data to test Vector.assert_all_valid *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_assert_valid" ~f:(fun res ->
      (* Fetch a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get the vector from the data chunk *)
      let vector =
        Duckdb_stubs.duckdb_data_chunk_get_vector
          !@(Resource.get_exn (Duckdb.Data_chunk.Private.to_ptr data_chunk))
          (Unsigned.UInt64.of_int 0)
      in
      (* Use Vector.assert_all_valid to check validity *)
      let is_valid =
        try
          Vector.assert_all_valid vector ~length:3;
          true
        with
        | _ -> false
      in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* Print the result *)
      [%message "Vector.assert_all_valid result" ~is_valid:(is_valid : bool)] |> print_s);
    [%expect.unreachable];
    (* Now test with NULL values *)
    Duckdb.Query.run_exn' conn {|CREATE TABLE test_assert_invalid(a INTEGER)|};
    Duckdb.Query.run_exn' conn {|INSERT INTO test_assert_invalid VALUES (1), (NULL), (3)|};
    (* Query the data to test Vector.assert_all_valid with NULLs *)
    Duckdb.Query.run_exn conn "SELECT * FROM test_assert_invalid" ~f:(fun res ->
      (* Fetch a data chunk *)
      let data_chunk =
        Duckdb.Result_.fetch res ~f:(fun opt ->
          Option.value_exn opt ~message:"Expected data chunk but got None")
      in
      (* Get the vector from the data chunk *)
      let vector =
        Duckdb_stubs.duckdb_data_chunk_get_vector
          !@(Resource.get_exn (Duckdb.Data_chunk.Private.to_ptr data_chunk))
          (Unsigned.UInt64.of_int 0)
      in
      (* Use Vector.assert_all_valid to check validity - should raise an exception *)
      let is_valid =
        try
          Vector.assert_all_valid vector ~length:3;
          true
        with
        | _ -> false
      in
      (* Free the data chunk *)
      Duckdb.Data_chunk.free data_chunk ~here:[%here];
      (* Print the result *)
      [%message "Vector.assert_all_valid with NULLs" ~is_valid:(is_valid : bool)]
      |> print_s);
    [%expect.unreachable])
[@@expect.uncaught_exn
  {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:49:71))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", line 354, characters 12-76
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Duckdb_test__Vector_test.(fun) in file "test/vector_test.ml", lines 345-368, characters 4-89
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;
