open! Core
open! Ctypes

(* Tests for Result_ module *)

let%expect_test "result_schema" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table with various types *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test_types(
        a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT,
        f FLOAT, g DOUBLE, h VARCHAR, i DATE, j TIMESTAMP
      )";
      
      (* Query the schema *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_types" ~f:(fun res ->
        (* Test schema function *)
        let schema = Duckdb.Result_.schema res in
        Array.iter schema ~f:(fun (name, type_) ->
          printf "%s: %s\n" name (Duckdb.Type.to_string type_));
        [%expect {|
          a: Boolean
          b: Tiny_int
          c: Small_int
          d: Integer
          e: Big_int
          f: Float
          g: Double
          h: Var_char
          i: Date
          j: Timestamp
        |}])))
;;

let%expect_test "result_fetch_all" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create and populate a test table *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test(a INTEGER, b VARCHAR)";
      Duckdb.Query.run_exn' conn "INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')";
      
      (* Test fetch_all *)
      Duckdb.Query.run_exn conn "SELECT * FROM test ORDER BY a" ~f:(fun res ->
        let columns = Duckdb.Result_.fetch_all res in
        
        (* Check first column (integers) *)
        (match columns.(0) with
         | Duckdb.Packed_column.T_non_null (Integer, values) ->
           printf "Integer column: %s\n" (Array.to_string values ~f:Int.to_string)
         | _ -> printf "Unexpected column type\n");
        [%expect {| Integer column: [1; 2; 3] |}];
        
        (* Check second column (varchars) *)
        (match columns.(1) with
         | Duckdb.Packed_column.T_non_null (Var_char, values) ->
           printf "Varchar column: %s\n" (Array.to_string values ~f:Fn.id)
         | _ -> printf "Unexpected column type\n");
        [%expect {| Varchar column: [one; two; three] |}])))
;;

let%expect_test "result_to_string_hum" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create and populate a test table *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test(a INTEGER, b VARCHAR)";
      Duckdb.Query.run_exn' conn "INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')";
      
      (* Test to_string_hum with different bar styles *)
      Duckdb.Query.run_exn conn "SELECT * FROM test ORDER BY a" ~f:(fun res ->
        printf "Unicode bars:\n%s\n" (Duckdb.Result_.to_string_hum res ~bars:`Unicode);
        printf "ASCII bars:\n%s\n" (Duckdb.Result_.to_string_hum res ~bars:`Ascii);
        printf "No bars:\n%s\n" (Duckdb.Result_.to_string_hum res ~bars:`None));
      [%expect {|
        Unicode bars:
        ┌─────────┬──────────┐
        │ a       │ b        │
        │ Integer │ Var_char │
        ├─────────┼──────────┤
        │ 1       │ one      │
        │ 2       │ two      │
        │ 3       │ three    │
        └─────────┴──────────┘
        
        ASCII bars:
        +---------+----------+
        | a       | b        |
        | Integer | Var_char |
        +---------+----------+
        | 1       | one      |
        | 2       | two      |
        | 3       | three    |
        +---------+----------+
        
        No bars:
        a       b        
        Integer Var_char 
        
        1       one      
        2       two      
        3       three    
        
      |}]))
;;
