open! Core
open! Ctypes

type t = Duckdb_stubs.duckdb_database ptr Resource.t

let open_exn path =
  let t =
    allocate
      Duckdb_stubs.duckdb_database
      (from_voidp Duckdb_stubs.duckdb_database_struct null)
  in
  match Duckdb_stubs.duckdb_open path t with
  | DuckDBSuccess ->
    Resource.create t ~name:"Duckdb.Database" ~free:Duckdb_stubs.duckdb_close
  | DuckDBError -> failwith "Failed to open database"
;;

let close = Resource.free
let with_path path ~f = open_exn path |> Exn.protectx ~f ~finally:(close ~here:[%here])

module Private = struct
  let to_ptr = Fn.id
end
