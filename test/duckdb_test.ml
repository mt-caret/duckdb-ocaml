open! Core
open! Ctypes

let%expect_test "duckdb_library_version" =
  Duckdb.duckdb_library_version () |> print_endline;
  [%expect {| v1.1.3 |}]
;;

let%expect_test "startup" =
  let db =
    allocate Duckdb.duckdb_database (from_voidp Duckdb.duckdb_database_struct null)
  in
  Duckdb.duckdb_open ":memory:" db |> [%sexp_of: Duckdb.duckdb_state] |> print_s;
  [%expect {| DuckDBSuccess |}];
  let conn =
    allocate Duckdb.duckdb_connection (from_voidp Duckdb.duckdb_connection_struct null)
  in
  Duckdb.duckdb_connect !@db conn |> [%sexp_of: Duckdb.duckdb_state] |> print_s;
  [%expect {| DuckDBSuccess |}];
  Duckdb.duckdb_disconnect conn;
  Duckdb.duckdb_close db
;;
