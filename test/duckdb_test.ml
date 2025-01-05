open! Core

let%expect_test "duckdb_library_version" =
  Duckdb.duckdb_library_version () |> print_endline;
  [%expect {| v1.1.3 |}]
;;

let%expect_test "open and close" =
  let db =
    Ctypes.allocate
      Duckdb.duckdb_database
      (Ctypes.from_voidp Duckdb.duckdb_database_struct Ctypes.null)
  in
  Duckdb.duckdb_open "test.db" db |> [%sexp_of: Duckdb.duckdb_state] |> print_s;
  [%expect {| DuckDBSuccess |}];
  Duckdb.duckdb_close db
;;
