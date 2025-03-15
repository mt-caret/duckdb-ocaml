open! Core

type t

val open_exn : string -> t
val close : t -> here:Source_code_position.t -> unit
val with_path : string -> f:(t -> 'a) -> 'a

module Private : sig
  val to_ptr : t -> Duckdb_stubs.Database.t Ctypes.ptr Resource.t
  val add_closure_root : 'a Resource.t -> t -> unit
end
