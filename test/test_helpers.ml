open! Core
open! Ctypes

let with_single_threaded_db f =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      f conn))
;;

let test_data_operations ~setup_sql ~query_sql ~f =
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn query_sql ~f)
;;

let print_result ?(bars = `Unicode) res =
  let result_str = Duckdb.Result_.to_string_hum res ~bars in
  print_endline result_str
;;
