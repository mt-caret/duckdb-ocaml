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
        Duckdb.Result_.schema res |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s)));
  [%expect
    {| ("Already freed" Duckdb.Database (first_freed_at test/duckdb_test.ml:LINE:COL)) |}]
;;

let%expect_test "try to use closed connection" =
  Expect_test_helpers_core.require_does_raise [%here] ~hide_positions:true (fun () ->
    let db = Duckdb.Database.open_exn ":memory:" in
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Connection.disconnect conn ~here:[%here];
      Duckdb.Query.run_exn conn "SELECT 1" ~f:(fun res ->
        Duckdb.Result_.schema res |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s)));
  [%expect
    {| ("Already freed" Duckdb.Connection (first_freed_at test/duckdb_test.ml:LINE:COL)) |}]
;;

let print_result result =
  Duckdb.Result_.to_string_hum result ~bars:`Unicode |> print_endline
;;

let%expect_test "create, insert, and select" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
      Duckdb.Query.run_exn' conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
      Duckdb.Query.run_exn conn "SELECT * FROM integers" ~f:print_result;
      [%expect
        {|
        ┌─────────┬─────────┐
        │ i       │ j       │
        │ Integer │ Integer │
        ├─────────┼─────────┤
        │ 3       │ 4       │
        │ 5       │ 6       │
        │ 7       │ null    │
        └─────────┴─────────┘
        |}]))
;;

let%expect_test "get type and name" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE integers (i INTEGER, j INTEGER)";
      Duckdb.Query.run_exn' conn "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)";
      Duckdb.Query.run_exn conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Result_.schema res |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s;
        [%expect {| ((i Integer) (j Integer)) |}])))
;;

let%expect_test "logical types" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn' conn "CREATE TABLE integers (i INTEGER[], j INTEGER[3])";
      Duckdb.Query.run_exn' conn "INSERT INTO integers VALUES ([3,4,5], [6,7,8])";
      Duckdb.Query.run_exn conn "SELECT * FROM integers" ~f:(fun res ->
        Duckdb.Result_.schema res |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s;
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
          Duckdb.Result_.schema res
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
        Duckdb.Result_.schema res |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s;
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
        Duckdb.Result_.schema res |> [%sexp_of: (string * Duckdb.Type.t) array] |> print_s)));
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

let%expect_test "scalar function registration" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      Duckdb.Scalar_function.create_exn
        "multiply_numbers_together"
        (Small_int :: Small_int :: Returning Small_int)
        ~f:( * )
        ~conn;
      Duckdb.Query.run_exn conn "SELECT multiply_numbers_together(2, 4)" ~f:print_result;
      [%expect
        {|
        ┌─────────────────────────────────┐
        │ multiply_numbers_together(2, 4) │
        │ Small_int                       │
        ├─────────────────────────────────┤
        │ 8                               │
        └─────────────────────────────────┘
        |}]))
;;

let%expect_test "scalar function raises an exception" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      Duckdb.Scalar_function.create_exn
        "multiply_numbers_together"
        (Small_int :: Small_int :: Returning Small_int)
        ~f:(fun _a _b -> raise_s [%message "This is a test exception"])
        ~conn;
      Expect_test_helpers_core.require_does_raise [%here] (fun () ->
        Duckdb.Query.run_exn conn "SELECT multiply_numbers_together(2, 4)" ~f:print_result);
      [%expect
        {|
        ((kind DUCKDB_ERROR_INVALID_INPUT)
         (message "Invalid Input Error: \"This is a test exception\""))
        |}]))
;;

let%expect_test "scalar function string concatenation" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      Duckdb.Scalar_function.create_exn
        "string_concat"
        (Var_char :: Var_char :: Returning Var_char)
        ~f:( ^ )
        ~conn;
      Duckdb.Query.run_exn conn "SELECT string_concat('hello', ' world')" ~f:print_result;
      [%expect
        {|
        ┌──────────────────────────────────┐
        │ string_concat('hello', ' world') │
        │ Var_char                         │
        ├──────────────────────────────────┤
        │ hello world                      │
        └──────────────────────────────────┘
        |}]))
;;

let%expect_test "scalar function tupling" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      Duckdb.Scalar_function.create_exn
        "string_concat"
        (Var_char :: Var_char :: Returning (List Var_char))
        ~f:(fun a b -> [ a; b ])
        ~conn;
      Duckdb.Query.run_exn conn "SELECT string_concat('hello', ' world')" ~f:print_result;
      [%expect
        {|
        ┌──────────────────────────────────┐
        │ string_concat('hello', ' world') │
        │ (List Var_char)                  │
        ├──────────────────────────────────┤
        │ [ hello,  world ]                │
        └──────────────────────────────────┘
        |}]))
;;

let%expect_test "test querying short and long strings" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn'
        conn
        "CREATE TABLE strings (short_string VARCHAR, long_string VARCHAR)";
      Duckdb.Query.run_exn' conn "INSERT INTO strings VALUES ('short', 'looooooooooong')";
      Duckdb.Query.run_exn conn "SELECT * FROM strings" ~f:print_result;
      [%expect
        {|
        ┌──────────────┬────────────────┐
        │ short_string │ long_string    │
        │ Var_char     │ Var_char       │
        ├──────────────┼────────────────┤
        │ short        │ looooooooooong │
        └──────────────┴────────────────┘
        |}]))
;;

(* TODO: A cleaner/safer API here would be nice. *)
let%expect_test "replacement scan" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Replacement_scan.add db ~f:(fun info ~table_name ->
      match Int.of_string_opt table_name with
      | None -> ()
      | Some n ->
        if n = 42 then raise_s [%message "42 is not a valid table name!"];
        Duckdb_stubs.duckdb_replacement_scan_set_function_name info "range";
        let value =
          allocate Duckdb_stubs.Value.t (Duckdb.Value.create_non_null Small_int n)
        in
        Duckdb_stubs.duckdb_replacement_scan_add_parameter info !@value;
        Duckdb_stubs.duckdb_destroy_value value);
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      Duckdb.Query.run_exn conn {|SELECT * FROM "5"|} ~f:print_result;
      [%expect
        {|
        ┌─────────┐
        │ range   │
        │ Big_int │
        ├─────────┤
        │ 0       │
        │ 1       │
        │ 2       │
        │ 3       │
        │ 4       │
        └─────────┘
        |}];
      Expect_test_helpers_core.require_does_raise [%here] (fun () ->
        Duckdb.Query.run_exn conn {|SELECT * FROM "42"|} ~f:print_result);
      [%expect
        {|
        ((kind DUCKDB_ERROR_BINDER)
         (message
          "Binder Error: Error in replacement scan: \"42 is not a valid table name!\"\n"))
        |}]))
;;

(* TODO: A cleaner/safer API here would be nice. *)
let%expect_test "table function registration" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      let size = ref 0 in
      let pos = ref 0 in
      Duckdb.Table_function.add
        "my_function"
        [ Big_int ]
        ~bind:(fun info ->
          print_endline "bind is called";
          [%test_result: int]
            (Duckdb_stubs.duckdb_bind_get_parameter_count info |> Unsigned.UInt64.to_int)
            ~expect:1;
          let value = Duckdb_stubs.duckdb_bind_get_parameter info Unsigned.UInt64.zero in
          let value = Duckdb.Value.get_non_null Big_int value in
          size := Int64.to_int_exn value;
          Duckdb.Type.to_logical_type Big_int
          |> Duckdb.Type.Private.with_logical_type ~f:(fun logical_type ->
            Duckdb_stubs.duckdb_bind_add_result_column info "forty_two" logical_type))
        ~init:(fun _info ->
          print_endline "init is called";
          pos := 0)
        ~f:(fun _info chunk ->
          print_endline "function is called";
          let vector =
            Duckdb_stubs.duckdb_data_chunk_get_vector chunk Unsigned.UInt64.zero
          in
          let chunk_size =
            min
              (!size - !pos)
              (* TODO: expose STANDARD_VECTOR_SIZE in wrapper library somehow. *)
              2048
          in
          Array.init chunk_size ~f:(fun i -> if (!pos + i) % 2 = 0 then 42L else 84L)
          |> Duckdb.Vector.set_array vector Big_int;
          Duckdb_stubs.duckdb_data_chunk_set_size
            chunk
            (Unsigned.UInt64.of_int chunk_size);
          pos := !pos + chunk_size)
        ~conn;
      (* Errors when trying to use as a scalar function *)
      Expect_test_helpers_core.require_does_raise [%here] (fun () ->
        Duckdb.Query.run_exn conn "SELECT my_function(1)" ~f:print_result);
      [%expect
        {|
        ((kind DUCKDB_ERROR_BINDER)
         (message
          "Binder Error: Function \"my_function\" is a table function but it was used as a scalar function. This function has to be called in a FROM clause (similar to a table).\n\nLINE 1: SELECT my_function(1)\n               ^"))
        |}];
      Duckdb.Query.run_exn conn "SELECT * FROM my_function(1)" ~f:print_result;
      [%expect
        {|
        bind is called
        init is called
        function is called
        function is called
        ┌───────────┐
        │ forty_two │
        │ Big_int   │
        ├───────────┤
        │ 42        │
        └───────────┘
        |}];
      (* Function can be called many times. *)
      Duckdb.Query.run_exn conn "SELECT * FROM my_function(4000)" ~f:(fun result ->
        let count, _rows = Duckdb.Result_.fetch_all result in
        print_s [%message "rows" (count : int)]);
      [%expect
        {|
        bind is called
        init is called
        function is called
        function is called
        function is called
        (rows (count 4000))
        |}]);
    (* Table function persists across connections *)
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Set DuckDB to single-threaded mode to avoid thread safety issues *)
      Single_thread_fix.set_single_threaded conn;
      Duckdb.Query.run_exn conn "SELECT * FROM my_function(1)" ~f:print_result;
      [%expect
        {|
        bind is called
        init is called
        function is called
        function is called
        ┌───────────┐
        │ forty_two │
        │ Big_int   │
        ├───────────┤
        │ 42        │
        └───────────┘
        |}]))
;;
