open! Core

type t

val fetch : Query.Result.t -> f:(t option -> 'a) -> 'a
val length : t -> int
val schema : t -> (string * Type.t) array
val get_exn : t -> 'a Type.Typed.t -> int -> 'a array
val get_opt : t -> 'a Type.Typed.t -> int -> 'a option array

(* TODO: this should belong elsewhere *)
val fetch_all : Query.Result.t -> int * (string * Packed_column.t) array

module Private : sig
  val to_ptr : t -> Duckdb_stubs.Data_chunk.t Ctypes.ptr
  val get_exn : Duckdb_stubs.Data_chunk.t -> 'a Type.Typed.t -> int -> 'a array
end
