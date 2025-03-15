open! Core
open! Ctypes

type t =
  { connection : Duckdb_stubs.Connection.t ptr Resource.t
  ; database : Database.t
  }

let connect_exn db =
  let db' = Database.Private.to_ptr db |> Resource.get_exn in
  let t =
    allocate Duckdb_stubs.Connection.t (from_voidp Duckdb_stubs.Connection.t_struct null)
  in
  match Duckdb_stubs.duckdb_connect !@db' t with
  | DuckDBSuccess ->
    { connection =
        Resource.create t ~name:"Duckdb.Connection" ~free:Duckdb_stubs.duckdb_disconnect
    ; database = db
    }
  | DuckDBError -> failwith "Failed to connect to database"
;;

let disconnect { connection; database = _ } = Resource.free connection

let with_connection db ~f =
  connect_exn db |> Exn.protectx ~f ~finally:(disconnect ~here:[%here])
;;

module Private = struct
  let to_ptr { connection; database = _ } = connection
  let database { connection = _; database } = database
end
