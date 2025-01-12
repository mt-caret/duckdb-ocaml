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

let duckdb_data_chunk_get_vector chunk col_idx =
  Duckdb.duckdb_data_chunk_get_vector !@chunk col_idx
;;

let%expect_test "create and insert" =
  let db = duckdb_open ":memory:" in
  let conn = duckdb_connect db in
  duckdb_query conn "CREATE TABLE test (id INTEGER, name TEXT)";
  duckdb_query conn "INSERT INTO test (id, name) VALUES (1, 'John')";
  Duckdb.duckdb_disconnect conn;
  Duckdb.duckdb_close db
;;

(* {[
  duckdb_database db;
  duckdb_connection con;
  duckdb_open(nullptr, &db);
  duckdb_connect(db, &con);

  duckdb_result res;
  duckdb_query(con, "CREATE TABLE integers (i INTEGER, j INTEGER);", NULL);
  duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL);
  duckdb_query(con, "SELECT * FROM integers;", &res);
]} *)

let%expect_test "create and insert" =
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
      let _vector = duckdb_data_chunk_get_vector chunk (Unsigned.UInt64.of_int 0) in
      Duckdb.duckdb_destroy_data_chunk chunk
    | None -> ());
  Duckdb.duckdb_disconnect conn;
  Duckdb.duckdb_close db
;;
