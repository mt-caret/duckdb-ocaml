open! Core
open! Ctypes

module F =
  (val Foreign.dynamic_funptr
         ~runtime_lock:true
         (Duckdb_stubs.Replacement_scan.Info.t @-> string @-> ptr void @-> returning void))

module Delete_callback =
  (val Foreign.dynamic_funptr ~runtime_lock:true (ptr void @-> returning void))

let add (db : Database.t) ~f =
  let db = Database.Private.to_ptr db |> Resource.get_exn in
  let f =
    F.of_fun (fun info table_name _extra_data ->
      try f info ~table_name with
      | exn ->
        let error =
          Exn.to_string_mach exn
          ^
          match Backtrace.Exn.most_recent_for_exn exn with
          | Some backtrace -> [%string "\n%{backtrace#Backtrace}"]
          | None -> ""
        in
        Duckdb_stubs.duckdb_replacement_scan_set_error info error)
  in
  let delete_callback = Delete_callback.of_fun (fun _ -> ()) in
  Duckdb_stubs.duckdb_add_replacement_scan
    !@db
    (Ctypes.coerce F.t Duckdb_stubs.Replacement_scan.callback f)
    None
    (Ctypes.coerce Delete_callback.t Duckdb_stubs.Delete_callback.t delete_callback)
;;
