open! Core
open! Ctypes

(* Tests for numeric types and operations, mimicking DuckDB's numeric tests *)

let%expect_test "numeric_type_casting" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create table with various numeric types *)
      Duckdb.Query.run_exn'
        conn
        "CREATE TABLE numeric_types (\n\
        \        a TINYINT, b SMALLINT, c INTEGER, d BIGINT,\n\
        \        e UTINYINT, f USMALLINT, g UINTEGER, h UBIGINT,\n\
        \        i FLOAT, j DOUBLE\n\
        \      )";
      (* Insert test values *)
      Duckdb.Query.run_exn'
        conn
        "INSERT INTO numeric_types VALUES \n\
        \        (1, 2, 3, 4, 5, 6, 7, 8, 1.5, 2.5),\n\
        \        (10, 1000, 100000, 1000000000, 200, 50000, 4000000000, 10000000000, \
         0.125, 0.0625)";
      (* Test casting between numeric types *)
      Duckdb.Query.run_exn
        conn
        {|SELECT 
          a::SMALLINT, b::INTEGER, c::BIGINT, 
          e::USMALLINT, f::UINTEGER, g::UBIGINT,
          i::DOUBLE, j::FLOAT
          FROM numeric_types ORDER BY a|}
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌───────────┬───────────┬───────────┬────────────┬────────────┬────────────┬──────────┬──────────┐
        │ a         │ b         │ c         │ e          │ f          │ g          │ i        │ j        │
        │ Small_int │ Integer   │ Big_int   │ U_small_int│ U_integer  │ U_big_int  │ Double   │ Float    │
        ├───────────┼───────────┼───────────┼────────────┼────────────┼────────────┼──────────┼──────────┤
        │ 1         │ 2         │ 3         │ 5          │ 6          │ 7          │ 1.5      │ 2.5      │
        │ 10        │ 1000      │ 100000    │ 200        │ 50000      │ 4000000000 │ 0.125    │ 0.0625   │
        └───────────┴───────────┴───────────┴────────────┴────────────┴────────────┴──────────┴──────────┘
        |}];
      (* Test numeric operations *)
      Duckdb.Query.run_exn
        conn
        {|SELECT 
          a + b, c - d, e * f, g / 2, i + j, j * 2
          FROM numeric_types ORDER BY a|}
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌───────────┬───────────┬────────────┬────────────┬──────────┬──────────┐
        │ a + b     │ c - d     │ e * f      │ g / 2      │ i + j    │ j * 2    │
        │ Integer   │ Big_int   │ U_integer  │ Double     │ Double   │ Double   │
        ├───────────┼───────────┼────────────┼────────────┼──────────┼──────────┤
        │ 3         │ -1        │ 30         │ 3.5        │ 4.0      │ 5.0      │
        │ 1010      │ -999900000│ 10000000   │ 2000000000 │ 0.1875   │ 0.125    │
        └───────────┴───────────┴────────────┴────────────┴──────────┴──────────┘
        |}];
      (* Test numeric functions *)
      Duckdb.Query.run_exn
        conn
        {|SELECT 
          ABS(-a), ROUND(i, 1), FLOOR(j), CEIL(i), 
          GREATEST(a, b), LEAST(c, d)
          FROM numeric_types ORDER BY a|}
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌───────────┬──────────┬──────────┬──────────┬───────────┬───────────┐
        │ abs(-a)   │ round(i,1)│ floor(j) │ ceil(i)  │ greatest(a,b)│ least(c,d)│
        │ Tiny_int  │ Double   │ Big_int  │ Big_int  │ Small_int │ Big_int   │
        ├───────────┼──────────┼──────────┼──────────┼───────────┼───────────┤
        │ 1         │ 1.5      │ 2        │ 2        │ 2         │ 3         │
        │ 10        │ 0.1      │ 0        │ 1        │ 1000      │ 100000    │
        └───────────┴──────────┴──────────┴──────────┴───────────┴───────────┘
        |}]))
;;

let%expect_test "numeric_overflow_handling" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Test integer overflow *)
      Duckdb.Query.run_exn
        conn
        {|SELECT 
          127::TINYINT + 1, 
          32767::SMALLINT + 1,
          255::UTINYINT + 1|}
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌───────────────┬─────────────────┬────────────────┐
        │ 127::TINYINT + 1│ 32767::SMALLINT + 1│ 255::UTINYINT + 1│
        │ Small_int     │ Integer         │ U_small_int    │
        ├───────────────┼─────────────────┼────────────────┤
        │ 128           │ 32768           │ 256            │
        └───────────────┴─────────────────┴────────────────┘
        |}];
      (* Test try_cast for handling invalid conversions *)
      Duckdb.Query.run_exn
        conn
        {|SELECT 
          TRY_CAST(1000 AS TINYINT),
          TRY_CAST(-1 AS UTINYINT),
          TRY_CAST('not a number' AS INTEGER)|}
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌─────────────────────────┬─────────────────────────┬───────────────────────────────┐
        │ try_cast(1000 AS TINYINT)│ try_cast(-1 AS UTINYINT)│ try_cast('not a number' AS INTEGER)│
        │ Tiny_int                │ U_tiny_int             │ Integer                      │
        ├─────────────────────────┼─────────────────────────┼───────────────────────────────┤
        │ null                    │ null                    │ null                          │
        └─────────────────────────┴─────────────────────────┴───────────────────────────────┘
        |}]))
;;
