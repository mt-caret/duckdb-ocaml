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
          Duckdb.Result_.fetch res ~f:(function
            | None -> failwith "Expected data chunk but got None"
            | Some chunk -> chunk)
        in
        (* Test length *)
        let length = Duckdb.Data_chunk.length data_chunk in
        [%message "Data chunk length" ~length:(length : int)] |> print_s;
        [%expect {| ("Data chunk length" (length 2)) |}];
        (* Test get_exn for integer column *)
        let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 in
        [%message "Integer column" ~values:(int_array : int32 array)] |> print_s;
        [%expect {| ("Integer column" (values (1 2))) |}];
        (* Test get_exn for varchar column *)
        let varchar_array = Duckdb.Data_chunk.get_exn data_chunk Var_char 1 in
        [%message "Varchar column" ~values:(varchar_array : string array)] |> print_s;
        [%expect {| ("Varchar column" (values (hello world))) |}];
        (* Test get_exn for double column *)
        let double_array = Duckdb.Data_chunk.get_exn data_chunk Double 2 in
        [%message "Double column" ~values:(double_array : float array)] |> print_s;
        [%expect {| ("Double column" (values (1.5 2.5))) |}];
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
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
          Duckdb.Result_.fetch res ~f:(function
            | None -> failwith "Expected data chunk but got None"
            | Some chunk -> chunk)
        in
        (* Test get_opt for integer column with NULL *)
        let int_array = Duckdb.Data_chunk.get_opt data_chunk Integer 0 in
        [%message "Integer column with NULL" ~values:(int_array : int32 option array)]
        |> print_s;
        [%expect {| ("Integer column with NULL" (values ((1) () (3)))) |}];
        (* Test get_opt for varchar column with NULL *)
        let varchar_array = Duckdb.Data_chunk.get_opt data_chunk Var_char 1 in
        [%message
          "Varchar column with NULL" ~values:(varchar_array : string option array)]
        |> print_s;
        [%expect {| ("Varchar column with NULL" (values ((hello) (world) ()))) |}];
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;
