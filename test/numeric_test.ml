open! Core
open! Ctypes

(* Tests for numeric types and operations *)

let%expect_test "integer_operations" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Test basic integer operations *)
      Duckdb.Query.run_exn
        conn
        "SELECT 1 + 2 as add, 5 - 3 as sub, 4 * 2 as mul, 10 / 2 as div"
        ~f:(fun res ->
          Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
         "\226\148\140\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\144\
        \n\226\148\130 add     \226\148\130 sub     \226\148\130 mul     \226\148\130 div    \226\148\130\
        \n\226\148\130 Integer \226\148\130 Integer \226\148\130 Integer \226\148\130 Double \226\148\130\
        \n\226\148\156\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\164\
        \n\226\148\130 3       \226\148\130 2       \226\148\130 8       \226\148\130 5.     \226\148\130\
        \n\226\148\148\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\152\
        \n"
        |}]))
;;

let%expect_test "float_operations" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Test basic float operations *)
      Duckdb.Query.run_exn
        conn
        "SELECT 1.5 + 2.5 as add, 5.5 - 3.2 as sub, 4.0 * 2.5 as mul, 10.0 / 2.0 as div"
        ~f:(fun res ->
          Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect.unreachable]))
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Unsupported type" (type_ (Decimal (width 3) (scale 1))))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Base__Array0.mapi in file "src/array0.ml", line 142, characters 24-46
  Called from Duckdb__Result_.fetch_all in file "src/wrapper/result_.ml", lines 44-51, characters 4-85
  Called from Duckdb__Result_.to_string_hum in file "src/wrapper/result_.ml", line 70, characters 24-35
  Called from Duckdb_test__Numeric_test.(fun) in file "test/numeric_test.ml", line 38, characters 10-57
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Duckdb_test__Numeric_test.(fun) in file "test/numeric_test.ml", lines 34-38, characters 6-91
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;

let%expect_test "numeric_type_conversion" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
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
        ~f:(fun res ->
          Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
         "\226\148\140\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\172\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\144\
        \n\226\148\130 tiny     \226\148\130 small     \226\148\130 int     \226\148\130 big     \226\148\130 float \226\148\130 double \226\148\130\
        \n\226\148\130 Tiny_int \226\148\130 Small_int \226\148\130 Integer \226\148\130 Big_int \226\148\130 Float \226\148\130 Double \226\148\130\
        \n\226\148\156\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\188\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\164\
        \n\226\148\130 42       \226\148\130 42        \226\148\130 42      \226\148\130 42      \226\148\130 42.   \226\148\130 42.    \226\148\130\
        \n\226\148\148\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\180\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\128\226\148\152\
        \n"
        |}]))
;;

let%expect_test "numeric_functions" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Test numeric functions *)
      Duckdb.Query.run_exn
        conn
        {|
        SELECT 
          ABS(-42) as abs_val,
          ROUND(42.4242, 2) as round_val,
          FLOOR(42.9) as floor_val,
          CEILING(42.1) as ceil_val
        |}
        ~f:(fun res ->
          Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect.unreachable]))
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)
  ("Unsupported type" (type_ (Decimal (width 6) (scale 2))))
  Raised at Base__Error.raise in file "src/error.ml" (inlined), line 9, characters 21-37
  Called from Base__Error.raise_s in file "src/error.ml", line 10, characters 26-47
  Called from Base__Array0.mapi in file "src/array0.ml", line 144, characters 21-43
  Called from Duckdb__Result_.fetch_all in file "src/wrapper/result_.ml", lines 44-51, characters 4-85
  Called from Duckdb__Result_.to_string_hum in file "src/wrapper/result_.ml", line 70, characters 24-35
  Called from Duckdb_test__Numeric_test.(fun) in file "test/numeric_test.ml", line 96, characters 10-57
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Duckdb__Query.run in file "src/wrapper/query.ml", lines 35-36, characters 4-70
  Called from Duckdb__Query.run_exn in file "src/wrapper/query.ml", line 43, characters 2-19
  Called from Duckdb_test__Numeric_test.(fun) in file "test/numeric_test.ml", lines 86-96, characters 6-91
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Base__Exn.protectx in file "src/exn.ml", line 79, characters 8-11
  Re-raised at Base__Exn.raise_with_original_backtrace in file "src/exn.ml" (inlined), line 59, characters 2-50
  Called from Base__Exn.protectx in file "src/exn.ml", line 86, characters 13-49
  Called from Ppx_expect_runtime__Test_block.Configured.dump_backtrace in file "runtime/test_block.ml", line 142, characters 10-28
  |}]
;;
