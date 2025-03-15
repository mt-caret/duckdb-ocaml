open! Core
open! Ctypes

module F =
  (val Foreign.dynamic_funptr
         ~runtime_lock:true
         (Duckdb_stubs.Function_info.t @-> Duckdb_stubs.Data_chunk.t @-> returning void))

module Bind_callback =
  (val Foreign.dynamic_funptr
         ~runtime_lock:true
         (Duckdb_stubs.Bind_info.t @-> returning void))

module Init_callback =
  (val Foreign.dynamic_funptr
         ~runtime_lock:true
         (Duckdb_stubs.Init_info.t @-> returning void))

let add name types ~bind ~init ~f ~conn =
  let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
  let table_function =
    allocate Duckdb_stubs.Table_function.t (Duckdb_stubs.duckdb_create_table_function ())
  in
  let bind =
    Bind_callback.of_fun (fun info ->
      try bind info with
      | exn ->
        let error =
          Exn.to_string_mach exn
          ^
          match Backtrace.Exn.most_recent_for_exn exn with
          | Some backtrace -> [%string "\n%{backtrace#Backtrace}"]
          | None -> ""
        in
        Duckdb_stubs.duckdb_bind_set_error info error)
  in
  let init =
    Init_callback.of_fun (fun info ->
      try init info with
      | exn ->
        let error =
          Exn.to_string_mach exn
          ^
          match Backtrace.Exn.most_recent_for_exn exn with
          | Some backtrace -> [%string "\n%{backtrace#Backtrace}"]
          | None -> ""
        in
        Duckdb_stubs.duckdb_init_set_error info error)
  in
  let f =
    F.of_fun (fun info chunk ->
      try f info chunk with
      | exn ->
        let error =
          Exn.to_string_mach exn
          ^
          match Backtrace.Exn.most_recent_for_exn exn with
          | Some backtrace -> [%string "\n%{backtrace#Backtrace}"]
          | None -> ""
        in
        Duckdb_stubs.duckdb_scalar_function_set_error info error)
  in
  Duckdb_stubs.duckdb_table_function_set_name !@table_function name;
  List.iter types ~f:(fun t ->
    Type.to_logical_type t
    |> Type.Private.with_logical_type ~f:(fun logical_type ->
      Duckdb_stubs.duckdb_table_function_add_parameter !@table_function logical_type));
  Duckdb_stubs.duckdb_table_function_set_bind
    !@table_function
    (Ctypes.coerce Bind_callback.t Duckdb_stubs.Table_function.bind bind);
  Duckdb_stubs.duckdb_table_function_set_init
    !@table_function
    (Ctypes.coerce Init_callback.t Duckdb_stubs.Table_function.init init);
  Duckdb_stubs.duckdb_table_function_set_function
    !@table_function
    (Ctypes.coerce F.t Duckdb_stubs.Table_function.function_ f);
  let status = Duckdb_stubs.duckdb_register_table_function !@conn !@table_function in
  Duckdb_stubs.duckdb_destroy_table_function table_function;
  match status with
  | DuckDBSuccess -> ()
  | DuckDBError -> failwith "Failed to register table function"
;;
