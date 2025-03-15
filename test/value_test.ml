open! Core
open! Ctypes

(* Tests for Value module, mimicking DuckDB's value tests *)

let%expect_test "value_create_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Create a scalar function that uses Value.create *)
      let scalar_function =
        Duckdb.Scalar_function.create
          "test_value_function"
          (Integer :: Returning Integer)
          ~f:(fun x -> Int32.(x * 2l))
      in
      Duckdb.Scalar_function.register_exn scalar_function conn;
      (* Test the function *)
      Duckdb.Query.run_exn conn "SELECT test_value_function(42)" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌─────────────────────┐
        │ test_value_function(42) │
        │ Integer            │
        ├─────────────────────┤
        │ 84                 │
        └─────────────────────┘
        |}]))
;;

let%expect_test "value_create_non_null" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Create a prepared statement that uses Value.create_non_null *)
      let prepared =
        Duckdb.Query.Prepared.create conn "SELECT ? + ?" |> Result.ok_or_failwith
      in
      (* Bind values using Value.create_non_null internally *)
      Duckdb.Query.Prepared.bind prepared [ Integer, 40l; Integer, 2l ]
      |> Result.ok_or_failwith;
      (* Execute the prepared statement *)
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌─────────┐
        │ ? + ?   │
        │ Integer │
        ├─────────┤
        │ 42      │
        └─────────┘
        |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared ~here:[%here]))
;;

let%expect_test "value_create_complex_types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Duckdb.Query.run_exn' conn "SET threads TO 1";
      (* Test list values *)
      let prepared =
        Duckdb.Query.Prepared.create conn "SELECT ? as list_value"
        |> Result.ok_or_failwith
      in
      (* Bind a list value *)
      Duckdb.Query.Prepared.bind prepared [ List Integer, [ 1l; 2l; 3l; 4l; 5l ] ]
      |> Result.ok_or_failwith;
      (* Execute the prepared statement *)
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> [%sexp_of: string] |> print_s);
      [%expect
        {|
        ┌───────────────┐
        │ list_value    │
        │ (List Integer)│
        ├───────────────┤
        │ [ 1, 2, 3, 4, 5 ]│
        └───────────────┘
        |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared ~here:[%here]))
;;
