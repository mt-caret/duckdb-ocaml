open! Core
open! Ctypes

type t = Duckdb_stubs.Result.t structure Resource.t

let create () =
  let result = make Duckdb_stubs.Result.t in
  Resource.create result ~name:"Duckdb.Resource" ~free:(fun result ->
    Duckdb_stubs.duckdb_destroy_result (addr result))
;;

let column_count (t : t) =
  Resource.get_exn t |> addr |> Duckdb_stubs.duckdb_column_count |> Unsigned.UInt64.to_int
;;

let schema (t : t) =
  let t' = Resource.get_exn t in
  column_count t
  |> Array.init ~f:(fun i ->
    let i = Unsigned.UInt64.of_int i in
    let name = Duckdb_stubs.duckdb_column_name (addr t') i in
    let type_ =
      Duckdb_stubs.duckdb_column_logical_type (addr t') i
      |> Type.Private.with_logical_type ~f:Type.of_logical_type_exn
    in
    name, type_)
;;

module Private = struct
  let to_struct = Fn.id
end
