open! Core

type t

val length : t -> int
val get_exn : t -> 'a Type.Typed_non_null.t -> int -> 'a array
val get_opt : t -> 'a Type.Typed.t -> int -> 'a option array
val free : t -> here:Source_code_position.t -> unit

(** Returns a human-readable string representation of the data chunk *)
val to_string_hum : ?bars:[ `Ascii | `Unicode ] -> column_count:int -> t -> string

module Private : sig
  val create : Duckdb_stubs.Data_chunk.t -> t
  val create_do_not_free : Duckdb_stubs.Data_chunk.t -> t
  val to_ptr : t -> Duckdb_stubs.Data_chunk.t Ctypes.ptr Resource.t
end
