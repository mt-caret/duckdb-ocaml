open! Core
open! Ctypes

(* Tests for numeric types and operations *)

let%expect_test "integer_operations" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Test basic integer operations *)
      Duckdb.Query.run_exn
        conn
        "SELECT 1 + 2 as add, 5 - 3 as sub, 4 * 2 as mul, 10 / 2 as div"
        ~f:Test_helpers.print_result;
      [%expect
        {|
        ┌─────────┬─────────┬─────────┬────────┐
        │ add     │ sub     │ mul     │ div    │
        │ Integer │ Integer │ Integer │ Double │
        ├─────────┼─────────┼─────────┼────────┤
        │ 3       │ 2       │ 8       │ 5.     │
        └─────────┴─────────┴─────────┴────────┘
        |}]))
;;

let%expect_test "float_operations" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Test basic float operations *)
      (* TODO: Add support for decimal types in duckdb-ocaml *)
      Expect_test_helpers_core.require_does_raise ~hide_positions:true [%here] (fun () ->
        Duckdb.Query.run_exn
          conn
          "SELECT 1.5 + 2.5 as add, 5.5 - 3.2 as sub, 4.0 * 2.5 as mul, 10.0 / 2.0 as div"
          ~f:Test_helpers.print_result);
      [%expect
        {|
        ("Unsupported type" (
          type_ (
            Decimal
            (width 3)
            (scale 1))))
        |}]))
;;

let%expect_test "numeric_type_conversion" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Test type conversions *)
      Duckdb.Query.run_exn
        conn
        {|
        SELECT 
          CAST(42 AS TINYINT) as tiny, 
          CAST(42 AS SMALLINT) as small,
          CAST(42 AS INTEGER) as int,
          CAST(42 AS BIGINT) as big,
          CAST(42 AS FLOAT) as float,
          CAST(42 AS DOUBLE) as double
        |}
        ~f:Test_helpers.print_result;
      [%expect
        {|
        ┌──────────┬───────────┬─────────┬─────────┬───────┬────────┐
        │ tiny     │ small     │ int     │ big     │ float │ double │
        │ Tiny_int │ Small_int │ Integer │ Big_int │ Float │ Double │
        ├──────────┼───────────┼─────────┼─────────┼───────┼────────┤
        │ 42       │ 42        │ 42      │ 42      │ 42.   │ 42.    │
        └──────────┴───────────┴─────────┴─────────┴───────┴────────┘
        |}]))
;;

let%expect_test "numeric_functions" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Test numeric functions *)
      (* TODO: Add support for decimal types in duckdb-ocaml *)
      Expect_test_helpers_core.require_does_raise ~hide_positions:true [%here] (fun () ->
        Duckdb.Query.run_exn
          conn
          {|
          SELECT 
            ABS(-42) as abs_val,
            ROUND(42.4242, 2) as round_val,
            FLOOR(42.9) as floor_val,
            CEILING(42.1) as ceil_val
          |}
          ~f:Test_helpers.print_result);
      [%expect
        {|
        ("Unsupported type" (
          type_ (
            Decimal
            (width 6)
            (scale 2))))
        |}]))
;;
