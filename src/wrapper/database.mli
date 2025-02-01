open! Core

type t

val open_exn : string -> t
val close : t -> here:Source_code_position.t -> unit
val with_path : string -> f:(t -> 'a) -> 'a

module Private : sig
  val to_ptr : t -> Duckdb_stubs.duckdb_database Ctypes.ptr Resource.t
end
