open! Core

type t

val connect_exn : Database.t -> t
val disconnect : t -> here:Source_code_position.t -> unit
val with_connection : Database.t -> f:(t -> 'a) -> 'a

module Private : sig
  val to_ptr : t -> Duckdb_stubs.Connection.t Ctypes.ptr Resource.t
end
