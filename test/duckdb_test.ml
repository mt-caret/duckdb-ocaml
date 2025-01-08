open! Core
open! Ctypes

let%expect_test "duckdb_library_version" =
  Duckdb.duckdb_library_version () |> print_endline;
  [%expect {| v1.1.3 |}]
;;

let duckdb_open path =
  let db =
    allocate Duckdb.duckdb_database (from_voidp Duckdb.duckdb_database_struct null)
  in
  match Duckdb.duckdb_open path db with
  | DuckDBSuccess -> db
  | DuckDBError -> failwith "Failed to open database"
;;

let duckdb_connect db =
  let conn =
    allocate Duckdb.duckdb_connection (from_voidp Duckdb.duckdb_connection_struct null)
  in
  match Duckdb.duckdb_connect !@db conn with
  | DuckDBSuccess -> conn
  | DuckDBError -> failwith "Failed to connect to database"
;;

let duckdb_query conn query =
  let duckdb_result = make Duckdb.duckdb_result in
  let state = Duckdb.duckdb_query !@conn query (Some (addr duckdb_result)) in
  Duckdb.duckdb_destroy_result (addr duckdb_result);
  match state with
  | DuckDBSuccess -> ()
  | DuckDBError -> failwith "Query failed"
;;

let%expect_test "create and insert" =
  let db = duckdb_open ":memory:" in
  let conn = duckdb_connect db in
  duckdb_query conn "CREATE TABLE test (id INTEGER, name TEXT)";
  duckdb_query conn "INSERT INTO test (id, name) VALUES (1, 'John')";
  Duckdb.duckdb_disconnect conn;
  Duckdb.duckdb_close db
;;
