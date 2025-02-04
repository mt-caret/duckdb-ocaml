open! Core
open! Ctypes

type t = Duckdb_stubs.Connection.t ptr Resource.t

let connect_exn db =
  let db = Database.Private.to_ptr db |> Resource.get_exn in
  let t =
    allocate Duckdb_stubs.Connection.t (from_voidp Duckdb_stubs.Connection.t_struct null)
  in
  match Duckdb_stubs.duckdb_connect !@db t with
  | DuckDBSuccess ->
    Resource.create t ~name:"Duckdb.Connection" ~free:Duckdb_stubs.duckdb_disconnect
  | DuckDBError -> failwith "Failed to connect to database"
;;

let disconnect = Resource.free

let with_connection db ~f =
  connect_exn db |> Exn.protectx ~f ~finally:(disconnect ~here:[%here])
;;

module Private = struct
  let to_ptr = Fn.id
end
