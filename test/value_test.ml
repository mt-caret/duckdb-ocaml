open! Core
open! Ctypes

(* Tests for Value module, mimicking DuckDB's value tests *)

let%expect_test "value_create_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* No need to set single-threaded mode for this test *)
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
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌─────────────────────────┐
        │ test_value_function(42) │
        │ Integer                 │
        ├─────────────────────────┤
        │ 84                      │
        └─────────────────────────┘
        |}]))
;;

let%expect_test "value_create_non_null" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* No need to set single-threaded mode for this test *)
      (* Create a prepared statement that uses Value.create_non_null *)
      let prepared = Duckdb.Query.Prepared.create_exn conn "SELECT ? + ?" in
      (* Bind values using Value.create_non_null internally *)
      Duckdb.Query.Prepared.bind_exn prepared [ Integer, 40l; Integer, 2l ];
      (* Execute the prepared statement *)
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────┐
        │ ($1 + $2) │
        │ Integer   │
        ├───────────┤
        │ 42        │
        └───────────┘
        |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared ~here:[%here]))
;;

let%expect_test "value_create_complex_types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* No need to set single-threaded mode for this test *)
      (* Test list values *)
      let prepared = Duckdb.Query.Prepared.create_exn conn "SELECT ? as list_value" in
      (* Bind a list value *)
      Duckdb.Query.Prepared.bind_exn prepared [ List Integer, [ 1l; 2l; 3l; 4l; 5l ] ];
      (* Execute the prepared statement *)
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect
        {|
        ┌───────────────────┐
        │ list_value        │
        │ (List Integer)    │
        ├───────────────────┤
        │ [ 1, 2, 3, 4, 5 ] │
        └───────────────────┘
        |}];
      (* Clean up *)
      Duckdb.Query.Prepared.destroy prepared ~here:[%here]))
;;
