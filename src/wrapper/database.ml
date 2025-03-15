open! Core
open! Ctypes

module Closure = struct
  type t = T : _ Resource.t -> t
end

type t =
  { database : Duckdb_stubs.Database.t ptr Resource.t
  ; mutable closure_roots : Closure.t list
  }

let open_exn path =
  let t =
    allocate Duckdb_stubs.Database.t (from_voidp Duckdb_stubs.Database.t_struct null)
  in
  match Duckdb_stubs.duckdb_open path t with
  | DuckDBSuccess ->
    { database = Resource.create t ~name:"Duckdb.Database" ~free:Duckdb_stubs.duckdb_close
    ; closure_roots = []
    }
  | DuckDBError -> failwith "Failed to open database"
;;

let close { database; closure_roots } ~here =
  Resource.free database ~here;
  List.iter closure_roots ~f:(fun (T resource) -> Resource.free resource ~here)
;;

let with_path path ~f = open_exn path |> Exn.protectx ~f ~finally:(close ~here:[%here])

module Private = struct
  let to_ptr { database; closure_roots = _ } = database

  let add_closure_root (type a) (f : a Resource.t) (t : t) =
    t.closure_roots <- T f :: t.closure_roots
  ;;
end
