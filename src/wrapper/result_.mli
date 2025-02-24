(* Called  [Query_result] instead of [Result] to avoid naming conflicts. *)
open! Core

type t

val create : unit -> t
val column_count : t -> int
val schema : t -> (string * Type.t) array
val fetch : t -> f:(Data_chunk.t option -> 'a) -> 'a
val fetch_all : t -> int * (string * Packed_column.t) array
val to_string_hum : ?bars:[ `Ascii | `Unicode ] -> t -> string

module Private : sig
  val to_struct : t -> Duckdb_stubs.Result.t Ctypes.structure Resource.t
end
