open! Core

let%expect_test "duckdb_library_version" =
  Duckdb.duckdb_library_version () |> print_endline;
  [%expect {| v1.1.3 |}]
;;
