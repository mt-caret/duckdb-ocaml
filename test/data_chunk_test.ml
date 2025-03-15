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
            Option.value_exn opt ~message:"Expected data chunk but got None")
        in
        (* Test length *)
        let length = Duckdb.Data_chunk.length data_chunk in
        [%message "Data chunk length" ~length:(length : int)] |> print_s;
        [%expect {| ("Data chunk length" (length 2)) |}];
        (* Get column data and print the results - need to copy data before the chunk is freed *)
        let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 |> Array.copy in
        let varchar_array =
          Duckdb.Data_chunk.get_exn data_chunk Var_char 1 |> Array.map ~f:String.copy
        in
        let double_array = Duckdb.Data_chunk.get_exn data_chunk Double 2 |> Array.copy in
        (* Print the results after the query completes and data_chunk is freed *)
        [%message "Integer column" ~values:(int_array : int32 array)] |> print_s;
        [%expect {| ("Integer column" (values (1l 2l))) |}];
        [%message "Varchar column" ~values:(varchar_array : string array)] |> print_s;
        [%expect {| ("Varchar column" (values ("hello" "world"))) |}];
        [%message "Double column" ~values:(double_array : float array)] |> print_s;
        [%expect {| ("Double column" (values (1.5 2.5))) |}])))
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
            Option.value_exn opt ~message:"Expected data chunk but got None")
        in
        (* Get column data and copy it before the chunk is freed *)
        let int_array =
          Duckdb.Data_chunk.get_opt data_chunk Integer 0
          |> Array.map ~f:(Option.map ~f:Fn.id)
        in
        let varchar_array =
          Duckdb.Data_chunk.get_opt data_chunk Var_char 1
          |> Array.map ~f:(Option.map ~f:String.copy)
        in
        (* Print the results after the query completes and data_chunk is freed *)
        [%message "Integer column with NULL" ~values:(int_array : int32 option array)]
        |> print_s;
        [%expect {| ("Integer column with NULL" (values ((Some 1l) None (Some 3l)))) |}];
        [%message
          "Varchar column with NULL" ~values:(varchar_array : string option array)]
        |> print_s;
        [%expect
          {| ("Varchar column with NULL" (values ((Some "hello") (Some "world") None))) |}])))
;;
