open! Core
open! Ctypes

let%expect_test "duckdb_library_version" =
  Duckdb_stubs.duckdb_library_version () |> print_endline;
  [%expect {| v1.1.3 |}]
;;

let duckdb_data_chunk_get_vector_uint32_t chunk col_idx ~row_count =
  let vector = Duckdb_stubs.duckdb_data_chunk_get_vector !@chunk col_idx in
  let data = Duckdb_stubs.duckdb_vector_get_data vector |> from_voidp uint32_t in
  match Duckdb_stubs.duckdb_vector_get_validity vector with
  | Some validity ->
    Array.init row_count ~f:(fun i ->
      match
        Duckdb_stubs.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i)
      with
      | true -> Some !@(data +@ i)
      | false -> None)
  | None -> Array.init row_count ~f:(fun i -> Some !@(data +@ i))
;;

let%expect_test "create, insert, and select" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run' conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
      Duckdb.Query.run' conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
      Duckdb.Query.run conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Data_chunk.fetch
          res
          ~f:
            (Option.iter ~f:(fun chunk ->
               let row_count = Duckdb.Data_chunk.length chunk in
               print_endline (Int.to_string row_count);
               [%expect {| 3 |}];
               let chunk = Duckdb.Data_chunk.Private.to_ptr chunk in
               let vector =
                 duckdb_data_chunk_get_vector_uint32_t
                   chunk
                   (Unsigned.UInt64.of_int 0)
                   ~row_count
                 |> Array.map ~f:(fun i -> Option.map ~f:Unsigned.UInt32.to_int i)
               in
               print_s [%message (vector : int option array)];
               [%expect {| (vector ((3) (5) (7))) |}];
               let vector =
                 duckdb_data_chunk_get_vector_uint32_t
                   chunk
                   (Unsigned.UInt64.of_int 1)
                   ~row_count
                 |> Array.map ~f:(fun i -> Option.map ~f:Unsigned.UInt32.to_int i)
               in
               print_s [%message (vector : int option array)];
               [%expect {| (vector ((4) (6) ())) |}])))))
;;

let%expect_test "get type and name" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run' conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
      Duckdb.Query.run' conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
      Duckdb.Query.run conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) list]
        |> print_s;
        [%expect {| ((i Integer) (j Integer)) |}])))
;;

let%expect_test "logical types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run' conn "CREATE TABLE integers (i INTEGER[], j INTEGER[3])";
      Duckdb.Query.run' conn "INSERT INTO integers VALUES ([3,4,5], [6,7,8])";
      Duckdb.Query.run conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) list]
        |> print_s;
        [%expect {| ((i (List Integer)) (j (Array Integer 3))) |}])))
;;

let%expect_test "nested logical types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run
        conn
        "SELECT {'birds': ['duck', 'goose', 'heron'], 'aliens': NULL, 'amphibians': \
         ['frog', 'toad']}"
        ~f:(fun res ->
          Duckdb.Query.Result.schema res
          |> [%sexp_of: (string * Duckdb.Type.t) list]
          |> print_s;
          [%expect
            {|
            (("main.struct_pack(birds := main.list_value('duck', 'goose', 'heron'), aliens := NULL, amphibians := main.list_value('frog', 'toad'))"
              (Struct
               ((birds (List Var_char)) (aliens Integer) (amphibians (List Var_char))))))
            |}])))
;;
