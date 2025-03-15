open! Core
open! Ctypes

(* Helper function to set DuckDB to single-threaded mode 
   This is necessary for tests that use OCaml callbacks with DuckDB to avoid segmentation faults *)
let set_single_threaded conn = Duckdb.Query.run_exn' conn "SET threads TO 1"

(* Helper function to set up single-threaded mode for all tests *)
let with_single_threaded_db f =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      set_single_threaded conn;
      f conn))
;;

(* Helper function to test data operations on a simple table *)
let test_data_operations ~setup_sql ~query_sql ~f =
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f)
;;

(* Helper function to print result as a string with specified bar style *)
let print_result ?(bars = `Unicode) res =
  let result_str = Duckdb.Result_.to_string_hum res ~bars in
  print_endline result_str
;;

(* Helper function to safely fetch a data chunk from a result *)
let fetch_chunk_exn res =
  Duckdb.Result_.fetch res ~f:(fun opt ->
    Option.value_exn opt ~message:"Expected data chunk but got None")
;;

(* Helper function to get data from a chunk and free it *)
let with_chunk_data res ~f =
  let chunk = fetch_chunk_exn res in
  (* Copy any data we need before freeing the chunk *)
  let result = f chunk in
  (* Don't free the chunk as it's already managed by the Result_ module *)
  result
;;
