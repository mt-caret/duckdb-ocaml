open! Core
open! Ctypes

(* Tests for Data_chunk module, mimicking DuckDB's data chunk tests *)

let%expect_test "data_chunk_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test(a INTEGER, b VARCHAR, c DOUBLE)|};
      Duckdb.Query.run_exn'
        conn
        {|INSERT INTO test VALUES (1, 'hello', 1.5), (2, 'world', 2.5)|};
      (* Run a query that returns a result *)
      Duckdb.Query.run_exn conn "SELECT * FROM test" ~f:(fun res ->
        (* Test data_chunk functionality *)
        let data_chunk =
          Duckdb.Result_.fetch res ~f:(fun opt ->
            match opt with
            | None -> failwith "Expected data chunk but got None"
            | Some chunk -> chunk)
        in
        (* Test length *)
        let length = Duckdb.Data_chunk.length data_chunk in
        [%message "Data chunk length" ~length:(length : int)] |> print_s;
        [%expect {| ("Data chunk length" (length 2)) |}];
        (* Get column data - note that we need to get all data before freeing the chunk *)
        let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 in
        let varchar_array = Duckdb.Data_chunk.get_exn data_chunk Var_char 1 in
        let double_array = Duckdb.Data_chunk.get_exn data_chunk Double 2 in
        (* Print the results before freeing the data chunk *)
        [%message "Integer column" ~values:(int_array : int32 array)] |> print_s;
        [%message "Varchar column" ~values:(varchar_array : string array)] |> print_s;
        [%message "Double column" ~values:(double_array : float array)] |> print_s;
        (* Free the data chunk *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:38:71))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb__Data_chunk.get_exn in file "src/wrapper/data_chunk.ml", line 14, characters 8-39
  Called from Duckdb_test__Data_chunk_test.(fun) in file "test/data_chunk_test.ml", line 28, characters 24-70
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
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;

let%expect_test "data_chunk_nulls" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table with NULL values *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE test_nulls(a INTEGER, b VARCHAR)|};
      Duckdb.Query.run_exn'
        conn
        {|INSERT INTO test_nulls VALUES (1, 'hello'), (NULL, 'world'), (3, NULL)|};
      (* Run a query that returns a result with NULLs *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_nulls" ~f:(fun res ->
        (* Test data_chunk functionality with NULLs *)
        let data_chunk =
          Duckdb.Result_.fetch res ~f:(fun opt ->
            match opt with
            | None -> failwith "Expected data chunk but got None"
            | Some chunk -> chunk)
        in
        (* Get column data - note that we need to get all data before freeing the chunk *)
        let int_array = Duckdb.Data_chunk.get_opt data_chunk Integer 0 in
        let varchar_array = Duckdb.Data_chunk.get_opt data_chunk Var_char 1 in
        (* Print the results before freeing the data chunk *)
        [%message "Integer column with NULL" ~values:(int_array : int32 option array)]
        |> print_s;
        [%message
          "Varchar column with NULL" ~values:(varchar_array : string option array)]
        |> print_s;
        (* Free the data chunk *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Already freed" Duckdb.Data_chunk
    (first_freed_at src/wrapper/result_.ml:38:71))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Duckdb__Data_chunk.get_opt in file "src/wrapper/data_chunk.ml", line 24, characters 8-39
  Called from Duckdb_test__Data_chunk_test.(fun) in file "test/data_chunk_test.ml", line 81, characters 24-70
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
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;
