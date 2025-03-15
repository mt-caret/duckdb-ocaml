open! Core
open! Ctypes
open Test_helpers

(* Tests for Data_chunk module, mimicking DuckDB's data chunk tests *)

let%expect_test "data_chunk_basic" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE
    );
    INSERT INTO test VALUES 
      (1, 'hello', 1.5), 
      (2, 'world', 2.5);|}
  in
  let query_sql = "SELECT * FROM test" in
  test_data_operations ~setup_sql ~query_sql ~f:print_result;
  [%expect
    {|
    ┌─────────┬──────────┬────────┐
    │ a       │ b        │ c      │
    │ Integer │ Var_char │ Double │
    ├─────────┼──────────┼────────┤
    │ 1       │ hello    │ 1.5    │
    │ 2       │ world    │ 2.5    │
    └─────────┴──────────┴────────┘
    |}]
;;

let%expect_test "data_chunk_nulls" =
  (* Create a test table with NULL values *)
  let setup_sql =
    {|CREATE TABLE test_nulls(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_nulls VALUES 
      (1, 'hello'), 
      (NULL, 'world'), 
      (3, NULL);|}
  in
  let query_sql = "SELECT * FROM test_nulls" in
  test_data_operations ~setup_sql ~query_sql ~f:print_result;
  [%expect
    {|
    ┌─────────┬──────────┐
    │ a       │ b        │
    │ Integer │ Var_char │
    ├─────────┼──────────┤
    │ 1       │ hello    │
    │ null    │ world    │
    │ 3       │ null     │
    └─────────┴──────────┘
    |}]
;;

let%expect_test "data_chunk_column_count" =
  (* Create a test table with multiple columns *)
  let setup_sql =
    {|CREATE TABLE test_columns(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE, 
      d BOOLEAN, 
      e DATE
    );
    INSERT INTO test_columns VALUES 
      (1, 'hello', 1.5, true, '2023-01-01');|}
  in
  let query_sql = "SELECT * FROM test_columns" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Test column_count function *)
    with_chunk_data res ~f:(fun _chunk ->
      let col_count = Duckdb.Result_.column_count res in
      let count = col_count in
      [%message "Data chunk column count" ~count:(count : int)] |> print_s));
  [%expect {| ("Data chunk column count" (count 5)) |}]
;;

let%expect_test "data_chunk_to_string_hum" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_display(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_display VALUES 
      (1, 'hello'), 
      (2, 'world');|}
  in
  let query_sql = "SELECT * FROM test_display" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    (* Test to_string_hum functionality *)
    let unicode_output = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
    let ascii_output = Duckdb.Result_.to_string_hum res ~bars:`Ascii in
    (* Print the results *)
    print_endline "Unicode bars:";
    print_endline unicode_output;
    print_endline "\nAscii bars:";
    print_endline ascii_output);
  [%expect
    {|
    Unicode bars:
    ┌─────────┬──────────┐
    │ a       │ b        │
    │ Integer │ Var_char │
    ├─────────┼──────────┤
    │ 1       │ hello    │
    │ 2       │ world    │
    └─────────┴──────────┘


    Ascii bars:
    |--------------------|
    | a       | b        |
    | Integer | Var_char |
    |---------+----------|
    |--------------------|
    |}]
;;

(* Test for Data_chunk.get_methods *)
let%expect_test "data_chunk_get_methods" =
  (* Create a test table with both valid and NULL values *)
  let setup_sql =
    {|CREATE TABLE test_get(
      a INTEGER, 
      b INTEGER
    );
    INSERT INTO test_get VALUES 
      (1, 1), 
      (2, NULL), 
      (3, 3);|}
  in
  (* Use separate queries for each column to avoid resource management issues *)
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    (* First query to get non-null values *)
    Duckdb.Query.run_exn conn "SELECT a FROM test_get" ~f:(fun res ->
      (* Actually test Data_chunk.get_exn by fetching a chunk and extracting values *)
      Duckdb.Result_.fetch res ~f:(function
        | None -> failwith "Expected data chunk but got None"
        | Some chunk ->
          (* Get the actual values using Data_chunk.get_exn *)
          let values = Duckdb.Data_chunk.get_exn chunk Integer 0 in
          (* Verify the values match what we expect *)
          let expected = [| 1l; 2l; 3l |] in
          for i = 0 to Array.length values - 1 do
            if not (Int32.equal values.(i) expected.(i))
            then
              failwith
                [%string
                  "Value mismatch at index %{i#Int}: expected %{expected.(i)#Int32}, got \
                   %{values.(i)#Int32}"]
          done;
          [%message "Data_chunk.get_exn verified" ~length:(Array.length values : int)]
          |> print_s));
    (* Second query to get nullable values *)
    Duckdb.Query.run_exn conn "SELECT b FROM test_get" ~f:(fun res ->
      (* Actually test Data_chunk.get_opt by fetching a chunk and extracting values *)
      Duckdb.Result_.fetch res ~f:(function
        | None -> failwith "Expected data chunk but got None"
        | Some chunk ->
          (* Get the actual values using Data_chunk.get_opt *)
          let values = Duckdb.Data_chunk.get_opt chunk Integer 0 in
          (* Verify the values match what we expect *)
          let expected = [| Some 1l; None; Some 3l |] in
          for i = 0 to Array.length values - 1 do
            match values.(i), expected.(i) with
            | None, None -> ()
            | Some v, Some e when Int32.equal v e -> ()
            | _ -> failwith [%string "Value mismatch at index %{i#Int}"]
          done;
          [%message "Data_chunk.get_opt verified" ~length:(Array.length values : int)]
          |> print_s)));
  [%expect
    {|
    ("Data_chunk.get_exn verified" (length 3))
    ("Data_chunk.get_opt verified" (length 3))
    |}]
;;

let%expect_test "data_chunk_column_count_function" =
  (* Create a test table with multiple columns *)
  let setup_sql =
    {|CREATE TABLE test_col_count(
      a INTEGER, 
      b VARCHAR, 
      c DOUBLE
    );
    INSERT INTO test_col_count VALUES (1, 'hello', 1.5);|}
  in
  let query_sql = "SELECT * FROM test_col_count" in
  test_data_operations ~setup_sql ~query_sql ~f:(fun res ->
    with_chunk_data res ~f:(fun _chunk ->
      (* Test column count *)
      let col_count = Duckdb.Result_.column_count res in
      [%message "Result column count" ~count:(col_count : int)] |> print_s));
  [%expect {| ("Result column count" (count 3)) |}]
;;

(* Test for Data_chunk.to_string_hum *)
let%expect_test "data_chunk_to_string_hum_function" =
  (* Create a test table *)
  let setup_sql =
    {|CREATE TABLE test_to_string(
      a INTEGER, 
      b VARCHAR
    );
    INSERT INTO test_to_string VALUES 
      (1, 'hello'), 
      (2, 'world');|}
  in
  with_single_threaded_db (fun conn ->
    Duckdb.Query.run_exn' conn setup_sql;
    Duckdb.Query.run_exn conn "SELECT * FROM test_to_string" ~f:(fun res ->
      let schema = Duckdb.Result_.schema res in
      Duckdb.Result_.fetch res ~f:(function
        | None -> failwith "Expected data chunk but got None"
        | Some chunk ->
          let unicode_output =
            Duckdb.Data_chunk.to_string_hum chunk ~schema ~bars:`Unicode
          in
          let ascii_output = Duckdb.Data_chunk.to_string_hum chunk ~schema ~bars:`Ascii in
          print_endline "Data_chunk.to_string_hum with Unicode bars:";
          print_endline unicode_output;
          print_endline "\nData_chunk.to_string_hum with Ascii bars:";
          print_endline ascii_output)));
  [%expect
    {|
    Data_chunk.to_string_hum with Unicode bars:
    ┌───┬───────┐
    │ a │ b     │
    ├───┼───────┤
    │ 1 │ hello │
    │ 2 │ world │
    └───┴───────┘


    Data_chunk.to_string_hum with Ascii bars:
    |-----------|
    | a | b     |
    |---+-------|
    | 1 | hello |
    | 2 | world |
    |-----------|
    |}]
;;
