open! Core
open! Ctypes

(* Tests for Data_chunk module, mimicking DuckDB's data chunk tests *)

let%expect_test "data_chunk_basic" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test(a INTEGER, b VARCHAR, c DOUBLE)";
      Duckdb.Query.run_exn' conn "INSERT INTO test VALUES (1, 'hello', 1.5), (2, 'world', 2.5)";
      
      (* Run a query that returns a result *)
      Duckdb.Query.run_exn conn "SELECT * FROM test" ~f:(fun res ->
        (* Test data_chunk functionality *)
        let data_chunk = Duckdb.Result_.Private.get_chunk res 0 in
        
        (* Test length *)
        let length = Duckdb.Data_chunk.length data_chunk in
        printf "Chunk length: %d\n" length;
        [%expect {| Chunk length: 2 |}];
        
        (* Test get_exn for integer column *)
        let int_array = Duckdb.Data_chunk.get_exn data_chunk Integer 0 in
        printf "Integer column: %s\n" (Array.to_string int_array ~f:Int.to_string);
        [%expect {| Integer column: [1; 2] |}];
        
        (* Test get_exn for varchar column *)
        let varchar_array = Duckdb.Data_chunk.get_exn data_chunk Var_char 1 in
        printf "Varchar column: %s\n" (Array.to_string varchar_array ~f:Fn.id);
        [%expect {| Varchar column: [hello; world] |}];
        
        (* Test get_exn for double column *)
        let double_array = Duckdb.Data_chunk.get_exn data_chunk Double 2 in
        printf "Double column: %s\n" (Array.to_string double_array ~f:Float.to_string);
        [%expect {| Double column: [1.5; 2.5] |}];
        
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;

let%expect_test "data_chunk_nulls" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a test table with NULL values *)
      Duckdb.Query.run_exn' conn "CREATE TABLE test_nulls(a INTEGER, b VARCHAR)";
      Duckdb.Query.run_exn' conn "INSERT INTO test_nulls VALUES (1, 'hello'), (NULL, 'world'), (3, NULL)";
      
      (* Run a query that returns a result with NULLs *)
      Duckdb.Query.run_exn conn "SELECT * FROM test_nulls" ~f:(fun res ->
        (* Test data_chunk functionality with NULLs *)
        let data_chunk = Duckdb.Result_.Private.get_chunk res 0 in
        
        (* Test get_opt for integer column with NULL *)
        let int_array = Duckdb.Data_chunk.get_opt data_chunk Integer 0 in
        printf "Integer column with NULL: %s\n" 
          (Array.to_string int_array ~f:(function 
            | None -> "NULL" 
            | Some i -> Int.to_string i));
        [%expect {| Integer column with NULL: [1; NULL; 3] |}];
        
        (* Test get_opt for varchar column with NULL *)
        let varchar_array = Duckdb.Data_chunk.get_opt data_chunk Var_char 1 in
        printf "Varchar column with NULL: %s\n" 
          (Array.to_string varchar_array ~f:(function 
            | None -> "NULL" 
            | Some s -> s));
        [%expect {| Varchar column with NULL: [hello; world; NULL] |}];
        
        (* Clean up *)
        Duckdb.Data_chunk.free data_chunk ~here:[%here])))
;;
