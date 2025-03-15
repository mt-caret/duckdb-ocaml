open! Core
open! Ctypes

(* Tests for Scalar_function module *)

let%expect_test "scalar_function_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Create a simple scalar function that doubles an integer *)
      let double_function =
        Duckdb.Scalar_function.create
          "double_int"
          (Integer :: Returning Integer)
          ~f:(fun x -> Int32.(x * 2l))
      in
      (* Register the function *)
      Duckdb.Scalar_function.register_exn double_function conn;
      (* Test the function *)
      Duckdb.Query.run_exn conn "SELECT double_int(42)" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌─────────────┐
        │ double_int(42) │
        │ Integer    │
        ├─────────────┤
        │ 84         │
        └─────────────┘
        |}]))
;;

let%expect_test "scalar_function_multiple_args" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Create a function that takes multiple arguments *)
      let add_function =
        Duckdb.Scalar_function.create
          "add_ints"
          (Integer :: Integer :: Returning Integer)
          ~f:(fun x y -> Int32.(x + y))
      in
      (* Register the function *)
      Duckdb.Scalar_function.register_exn add_function conn;
      (* Test the function *)
      Duckdb.Query.run_exn conn "SELECT add_ints(40, 2)" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌───────────────┐
        │ add_ints(40, 2) │
        │ Integer      │
        ├───────────────┤
        │ 42           │
        └───────────────┘
        |}]))
;;

let%expect_test "scalar_function_null_handling" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Create a function that handles NULL values *)
      let handle_null_function =
        Duckdb.Scalar_function.create
          "handle_null"
          (Integer :: Returning Integer)
          ~f:(fun x -> Int32.(x * 2l))
      in
      (* Register the function *)
      Duckdb.Scalar_function.register_exn handle_null_function conn;
      (* Test the function with NULL input *)
      Duckdb.Query.run_exn conn "SELECT handle_null(NULL)" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌──────────────┐
        │ handle_null(NULL) │
        │ Integer     │
        ├──────────────┤
        │ NULL        │
        └──────────────┘
        |}]))
;;
