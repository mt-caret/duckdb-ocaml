open! Core
open! Ctypes

(* Tests for DuckDB functions, mimicking DuckDB's function tests *)

let%expect_test "aggregate_functions" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table *)
      Duckdb.Query.run_exn' conn "CREATE TABLE numbers(a INTEGER, b DOUBLE, c VARCHAR)";
      (* Insert test data *)
      Duckdb.Query.run_exn'
        conn
        "INSERT INTO numbers VALUES \n\
        \        (1, 1.5, 'one'),\n\
        \        (2, 2.5, 'two'),\n\
        \        (3, 3.5, 'three'),\n\
        \        (NULL, NULL, NULL),\n\
        \        (5, 5.5, 'five')";
      (* Test COUNT function *)
      Duckdb.Query.run_exn
        conn
        "SELECT COUNT(*), COUNT(a), COUNT(DISTINCT a) FROM numbers"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌──────────┬──────────┬────────────────┐
        │ count(*) │ count(a) │ count(DISTINCT a)│
        │ Big_int  │ Big_int  │ Big_int        │
        ├──────────┼──────────┼────────────────┤
        │ 5        │ 4        │ 4              │
        └──────────┴──────────┴────────────────┘
      |}];
      (* Test SUM, AVG, MIN, MAX functions *)
      Duckdb.Query.run_exn
        conn
        "SELECT \n\
        \        SUM(a), AVG(a), MIN(a), MAX(a),\n\
        \        SUM(b), AVG(b), MIN(b), MAX(b)\n\
        \        FROM numbers"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐
        │ sum(a) │ avg(a) │ min(a) │ max(a) │ sum(b) │ avg(b) │ min(b) │ max(b) │
        │ Big_int│ Double │ Integer│ Integer│ Double │ Double │ Double │ Double │
        ├────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┤
        │ 11     │ 2.75   │ 1      │ 5      │ 13.0   │ 3.25   │ 1.5    │ 5.5    │
        └────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘
      |}];
      (* Test string functions *)
      Duckdb.Query.run_exn
        conn
        "SELECT \n\
        \        STRING_AGG(c, ', '), \n\
        \        MIN(c), \n\
        \        MAX(c)\n\
        \        FROM numbers\n\
        \        WHERE c IS NOT NULL"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────────────────┬────────┬────────┐
        │ string_agg(c, ', ')   │ min(c) │ max(c) │
        │ Var_char              │ Var_char│ Var_char│
        ├───────────────────────┼────────┼────────┤
        │ one, two, three, five │ five   │ two    │
        └───────────────────────┴────────┴────────┘
      |}]))
;;

let%expect_test "case_expressions" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test(a INTEGER, b VARCHAR)";
      (* Insert test data *)
      Duckdb.Query.run_exn'
        conn
        "INSERT INTO test VALUES \n\
        \        (1, 'one'),\n\
        \        (2, 'two'),\n\
        \        (3, 'three'),\n\
        \        (NULL, NULL)";
      (* Test simple CASE expression *)
      Duckdb.Query.run_exn
        conn
        "SELECT a, \n\
        \        CASE a \n\
        \          WHEN 1 THEN 'first' \n\
        \          WHEN 2 THEN 'second' \n\
        \          WHEN 3 THEN 'third' \n\
        \          ELSE 'other' \n\
        \        END as case_result\n\
        \        FROM test\n\
        \        ORDER BY a NULLS LAST"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────┬────────────┐
        │ a         │ case_result│
        │ Integer   │ Var_char   │
        ├───────────┼────────────┤
        │ 1         │ first      │
        │ 2         │ second     │
        │ 3         │ third      │
        │ null      │ other      │
        └───────────┴────────────┘
      |}];
      (* Test searched CASE expression *)
      Duckdb.Query.run_exn
        conn
        "SELECT a, b,\n\
        \        CASE \n\
        \          WHEN a < 2 THEN 'low' \n\
        \          WHEN a < 3 THEN 'medium' \n\
        \          WHEN a >= 3 THEN 'high' \n\
        \          ELSE 'unknown' \n\
        \        END as range_result\n\
        \        FROM test\n\
        \        ORDER BY a NULLS LAST"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────┬──────────┬─────────────┐
        │ a         │ b        │ range_result│
        │ Integer   │ Var_char │ Var_char    │
        ├───────────┼──────────┼─────────────┤
        │ 1         │ one      │ low         │
        │ 2         │ two      │ medium      │
        │ 3         │ three    │ high        │
        │ null      │ null     │ unknown     │
        └───────────┴──────────┴─────────────┘
      |}]))
;;

let%expect_test "scalar_functions" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      (* Test string functions *)
      Duckdb.Query.run_exn
        conn
        "SELECT \n\
        \        CONCAT('hello', ' ', 'world') as concat_result,\n\
        \        UPPER('hello') as upper_result,\n\
        \        LOWER('WORLD') as lower_result,\n\
        \        LENGTH('hello world') as length_result,\n\
        \        SUBSTRING('hello world', 7, 5) as substring_result"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────────┬─────────────┬─────────────┬───────────────┬─────────────────┐
        │ concat_result │ upper_result│ lower_result│ length_result │ substring_result│
        │ Var_char      │ Var_char    │ Var_char    │ Integer       │ Var_char        │
        ├───────────────┼─────────────┼─────────────┼───────────────┼─────────────────┤
        │ hello world   │ HELLO       │ world       │ 11            │ world           │
        └───────────────┴─────────────┴─────────────┴───────────────┴─────────────────┘
      |}];
      (* Test math functions *)
      Duckdb.Query.run_exn
        conn
        "SELECT \n\
        \        ABS(-5) as abs_result,\n\
        \        ROUND(3.14159, 2) as round_result,\n\
        \        CEIL(3.14) as ceil_result,\n\
        \        FLOOR(3.99) as floor_result,\n\
        \        SQRT(16) as sqrt_result,\n\
        \        POWER(2, 3) as power_result"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────┬─────────────┬────────────┬─────────────┬────────────┬─────────────┐
        │ abs_result│ round_result│ ceil_result│ floor_result│ sqrt_result│ power_result│
        │ Integer   │ Double      │ Big_int    │ Big_int     │ Double     │ Double      │
        ├───────────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
        │ 5         │ 3.14        │ 4          │ 3           │ 4.0        │ 8.0         │
        └───────────┴─────────────┴────────────┴─────────────┴────────────┴─────────────┘
      |}];
      (* Test date/time functions *)
      Duckdb.Query.run_exn
        conn
        "SELECT \n\
        \        CURRENT_DATE() as today,\n\
        \        DATE_PART('year', DATE '2023-05-15') as year_part,\n\
        \        DATE_PART('month', DATE '2023-05-15') as month_part,\n\
        \        DATE_PART('day', DATE '2023-05-15') as day_part,\n\
        \        DATE_DIFF('day', DATE '2023-01-01', DATE '2023-01-10') as date_diff"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌────────────┬───────────┬────────────┬──────────┬───────────┐
        │ today      │ year_part │ month_part │ day_part │ date_diff │
        │ Date       │ Integer   │ Integer    │ Integer  │ Integer   │
        ├────────────┼───────────┼────────────┼──────────┼───────────┤
        │ 2025-03-14 │ 2023      │ 5          │ 15       │ 9         │
        └────────────┴───────────┴────────────┴──────────┴───────────┘
      |}];
      (* Test custom scalar function *)
      let scalar_function =
        Duckdb.Scalar_function.create
          "custom_multiply"
          (Integer :: Integer :: Returning Integer)
          ~f:( * )
      in
      Duckdb.Scalar_function.register_exn scalar_function conn;
      Duckdb.Query.run_exn
        conn
        "SELECT custom_multiply(5, 7) as custom_result"
        ~f:(fun res -> Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────────┐
        │ custom_result │
        │ Integer       │
        ├───────────────┤
        │ 35            │
        └───────────────┘
      |}]))
;;
