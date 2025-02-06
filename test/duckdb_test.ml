open! Core
open! Ctypes

let%expect_test "duckdb_library_version" =
  Duckdb_stubs.duckdb_library_version () |> print_endline;
  [%expect {| v1.2.0 |}]
;;

let%expect_test "try to use closed database" =
  Expect_test_helpers_core.require_does_raise [%here] ~hide_positions:true (fun () ->
    let db = Duckdb.Database.open_exn ":memory:" in
    Duckdb.Database.close db ~here:[%here];
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn conn "SELECT 1" ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) array]
        |> print_s)));
  [%expect
    {| ("Already freed" Duckdb.Database (first_freed_at test/duckdb_test.ml:LINE:COL)) |}]
;;

let%expect_test "try to use closed connection" =
  Expect_test_helpers_core.require_does_raise [%here] ~hide_positions:true (fun () ->
    let db = Duckdb.Database.open_exn ":memory:" in
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Connection.disconnect conn ~here:[%here];
      Duckdb.Query.run_exn conn "SELECT 1" ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) array]
        |> print_s)));
  [%expect
    {| ("Already freed" Duckdb.Connection (first_freed_at test/duckdb_test.ml:LINE:COL)) |}]
;;

let%expect_test "create, insert, and select" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
      Duckdb.Query.run_exn' conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
      Duckdb.Query.run_exn conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Data_chunk.fetch
          res
          ~f:
            (Option.iter ~f:(fun chunk ->
               let row_count = Duckdb.Data_chunk.length chunk in
               print_endline (Int.to_string row_count);
               [%expect {| 3 |}];
               let vector =
                 Duckdb.Data_chunk.get_exn chunk U_integer 0
                 |> Array.map ~f:Unsigned.UInt32.to_int
               in
               print_s [%message (vector : int array)];
               [%expect {| (vector (3 5 7)) |}];
               let vector =
                 Duckdb.Data_chunk.get_opt chunk U_integer 1
                 |> Array.map ~f:(Option.map ~f:Unsigned.UInt32.to_int)
               in
               print_s [%message (vector : int option array)];
               [%expect {| (vector ((4) (6) ())) |}])))))
;;

let%expect_test "get type and name" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
      Duckdb.Query.run_exn' conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
      Duckdb.Query.run_exn conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) array]
        |> print_s;
        [%expect {| ((i Integer) (j Integer)) |}])))
;;

let%expect_test "logical types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE integers (i INTEGER[], j INTEGER[3])";
      Duckdb.Query.run_exn' conn "INSERT INTO integers VALUES ([3,4,5], [6,7,8])";
      Duckdb.Query.run_exn conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) array]
        |> print_s;
        [%expect {| ((i (List Integer)) (j (Array Integer 3))) |}])))
;;

let%expect_test "nested logical types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn
        conn
        "SELECT {'birds': ['duck', 'goose', 'heron'], 'aliens': NULL, 'amphibians': \
         ['frog', 'toad']}"
        ~f:(fun res ->
          Duckdb.Query.Result.schema res
          |> [%sexp_of: (string * Duckdb.Type.t) array]
          |> print_s;
          [%expect
            {|
            (("main.struct_pack(birds := main.list_value('duck', 'goose', 'heron'), aliens := NULL, amphibians := main.list_value('frog', 'toad'))"
              (Struct
               ((birds (List Var_char)) (aliens Integer) (amphibians (List Var_char))))))
            |}])))
;;

let%expect_test "query errors" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run' conn "SELECT * FROM non_existent_table"
      |> [%sexp_of: (unit, Duckdb.Query.Error.t) result]
      |> print_s;
      [%expect
        {|
        (Error
         ((kind DUCKDB_ERROR_CATALOG)
          (message
            "Catalog Error: Table with name non_existent_table does not exist!\
           \nDid you mean \"sqlite_temp_master\"?\
           \n\
           \nLINE 1: SELECT * FROM non_existent_table\
           \n                      ^")))
        |}]))
;;

let%expect_test "prepared statements" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      let prepared =
        Duckdb.Query.Prepared.create conn "SELECT (1, 2, 3) WHERE 1 = ?"
        |> Result.ok_or_failwith
      in
      (* Query fails when no parameters are bound *)
      Duckdb.Query.Prepared.run' prepared
      |> [%sexp_of: (unit, Duckdb.Query.Error.t) result]
      |> print_s;
      [%expect
        {|
        (Error
         ((kind DUCKDB_ERROR_INVALID_INPUT)
          (message
           "Invalid Input Error: Values were not provided for the following prepared statement parameters: 1")))
        |}];
      (* Query succeeds when parameters are bound *)
      Duckdb.Query.Prepared.bind prepared [ Small_int, 1 ]
      |> [%sexp_of: (unit, string) result]
      |> print_s;
      [%expect {| (Ok ()) |}];
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) array]
        |> print_s;
        [%expect
          {| (("main.\"row\"(1, 2, 3)" (Struct (("" Integer) ("" Integer) ("" Integer))))) |}]);
      Duckdb.Query.Prepared.destroy prepared ~here:[%here]))
;;

let%expect_test "try to use closed prepared statement" =
  Expect_test_helpers_core.require_does_raise [%here] ~hide_positions:true (fun () ->
    let db = Duckdb.Database.open_exn ":memory:" in
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      let prepared =
        Duckdb.Query.Prepared.create conn "SELECT (1, 2, 3) WHERE 1 = ?"
        |> Result.ok_or_failwith
      in
      Duckdb.Query.Prepared.destroy prepared ~here:[%here];
      Duckdb.Query.Prepared.run_exn prepared ~f:(fun res ->
        Duckdb.Query.Result.schema res
        |> [%sexp_of: (string * Duckdb.Type.t) array]
        |> print_s)));
  [%expect
    {| ("Already freed" Duckdb.Prepared (first_freed_at test/duckdb_test.ml:LINE:COL)) |}]
;;

let%expect_test "appender" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE tbl (a SMALLINT, b BOOLEAN, c FLOAT)";
      let appender = Duckdb.Appender.create conn "tbl" in
      Duckdb.Appender.append_exn
        appender
        [ Small_int; Boolean; Float ]
        [ [ 1; true; 0.5 ]; [ 2; false; 1.0 ]; [ 3; true; 1.5 ] ];
      (* Pass wrong types *)
      Expect_test_helpers_core.require_does_raise [%here] (fun () ->
        Duckdb.Appender.append_exn appender [ Small_int; Float ] [ [ 1; 0.5 ] ]);
      [%expect
        {|
        ("Column types do not match"
          (expected_types (Small_int Boolean Float))
          (argument_types (Small_int Float)))
        |}]))
;;
