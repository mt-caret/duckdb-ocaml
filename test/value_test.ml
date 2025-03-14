open! Core
open! Ctypes

(* Tests for Value module, mimicking DuckDB's value tests *)

let%expect_test "value_create_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      
      (* Create a scalar function that uses Value.create *)
      let scalar_function =
        Duckdb.Scalar_function.create
          "test_value_function"
          (Integer :: Returning Integer)
          ~f:(fun x -> x * 2)
      in
      Duckdb.Scalar_function.register_exn scalar_function conn;
      
      (* Test the function *)
      Duckdb.Query.run_exn conn "SELECT test_value_function(42)" ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect {|
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
      Single_thread_fix.set_single_threaded conn;
      
      (* Create a prepared statement that uses Value.create_non_null *)
      let prepared =
        Duckdb.Query.Prepared.create conn "SELECT ? + ?"
        |> Result.ok_or_failwith
      in
      
      (* Bind values using Value.create_non_null internally *)
      Duckdb.Query.Prepared.bind prepared [ Integer, 40; Integer, 2 ]
      |> Result.ok_or_failwith;
      
      (* Execute the prepared statement *)
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect {|
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
      Single_thread_fix.set_single_threaded conn;
      
      (* Test list values *)
      let prepared =
        Duckdb.Query.Prepared.create conn "SELECT ? as list_value"
        |> Result.ok_or_failwith
      in
      
      (* Bind a list value *)
      Duckdb.Query.Prepared.bind prepared [ List Integer, [ 1; 2; 3; 4; 5 ] ]
      |> Result.ok_or_failwith;
      
      (* Execute the prepared statement *)
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Result_.to_string_hum res ~bars:`Unicode |> print_endline);
      [%expect {|
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
