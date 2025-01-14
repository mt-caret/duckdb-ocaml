open! Core
open! Ctypes

let%expect_test "duckdb_library_version" =
  Duckdb.duckdb_library_version () |> print_endline;
  [%expect {| v1.1.3 |}]
;;

let duckdb_open path =
  let db =
    allocate Duckdb.duckdb_database (from_voidp Duckdb.duckdb_database_struct null)
  in
  match Duckdb.duckdb_open path db with
  | DuckDBSuccess -> db
  | DuckDBError -> failwith "Failed to open database"
;;

let duckdb_connect db =
  let conn =
    allocate Duckdb.duckdb_connection (from_voidp Duckdb.duckdb_connection_struct null)
  in
  match Duckdb.duckdb_connect !@db conn with
  | DuckDBSuccess -> conn
  | DuckDBError -> failwith "Failed to connect to database"
;;

let duckdb_query ?f conn query =
  let duckdb_result = make Duckdb.duckdb_result in
  let state = Duckdb.duckdb_query !@conn query (Some (addr duckdb_result)) in
  match state with
  | DuckDBError ->
    Duckdb.duckdb_destroy_result (addr duckdb_result);
    failwith "Query failed"
  | DuckDBSuccess ->
    (match f with
     | Some f ->
       (try f duckdb_result with
        | e ->
          Duckdb.duckdb_destroy_result (addr duckdb_result);
          raise e)
     | None -> Duckdb.duckdb_destroy_result (addr duckdb_result))
;;

let duckdb_fetch_chunk res =
  Duckdb.duckdb_fetch_chunk res
  |> Option.map ~f:(fun chunk -> allocate Duckdb.duckdb_data_chunk chunk)
;;

let duckdb_data_chunk_get_vector_uint32_t chunk col_idx ~row_count =
  let vector = Duckdb.duckdb_data_chunk_get_vector !@chunk col_idx in
  let data = Duckdb.duckdb_vector_get_data vector |> from_voidp uint32_t in
  match Duckdb.duckdb_vector_get_validity vector with
  | Some validity ->
    Array.init row_count ~f:(fun i ->
      match Duckdb.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i) with
      | true -> Some !@(data +@ i)
      | false -> None)
  | None -> Array.init row_count ~f:(fun i -> Some !@(data +@ i))
;;

let%expect_test "create, insert, and select" =
  let db = duckdb_open ":memory:" in
  let conn = duckdb_connect db in
  duckdb_query conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
  duckdb_query conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
  duckdb_query conn "SELECT * FROM integers" ~f:(fun res ->
    let chunk = duckdb_fetch_chunk res in
    match chunk with
    | Some chunk ->
      let row_count = Duckdb.duckdb_data_chunk_get_size !@chunk in
      print_endline (Int.to_string (Unsigned.UInt64.to_int row_count));
      [%expect {| 3 |}];
      let vector =
        duckdb_data_chunk_get_vector_uint32_t
          chunk
          (Unsigned.UInt64.of_int 0)
          ~row_count:(Unsigned.UInt64.to_int row_count)
        |> Array.map ~f:(fun i -> Option.map ~f:Unsigned.UInt32.to_int i)
      in
      print_s [%message (vector : int option array)];
      [%expect {| (vector ((3) (5) (7))) |}];
      let vector =
        duckdb_data_chunk_get_vector_uint32_t
          chunk
          (Unsigned.UInt64.of_int 1)
          ~row_count:(Unsigned.UInt64.to_int row_count)
        |> Array.map ~f:(fun i -> Option.map ~f:Unsigned.UInt32.to_int i)
      in
      print_s [%message (vector : int option array)];
      [%expect {| (vector ((4) (6) ())) |}];
      Duckdb.duckdb_destroy_data_chunk chunk
    | None -> ());
  Duckdb.duckdb_disconnect conn;
  Duckdb.duckdb_close db
;;
