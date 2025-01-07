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

let%expect_test "create and insert" =
  let db = duckdb_open ":memory:" in
  let conn = duckdb_connect db in
  Duckdb.duckdb_query !@conn "CREATE TABLE test (id INTEGER, name TEXT)" None
  |> [%sexp_of: Duckdb.duckdb_state]
  |> print_s;
  [%expect {| DuckDBSuccess |}];
  Duckdb.duckdb_query !@conn "INSERT INTO test (id, name) VALUES (1, 'John')" None
  |> [%sexp_of: Duckdb.duckdb_state]
  |> print_s;
  [%expect {| DuckDBSuccess |}];
  Duckdb.duckdb_disconnect conn;
  Duckdb.duckdb_close db
;;
