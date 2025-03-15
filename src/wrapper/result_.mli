(* Called  [Query_result] instead of [Result] to avoid naming conflicts. *)
open! Core

type t

val create : unit -> t
val column_count : t -> int
(** Returns the number of columns in the result. *)

val schema : t -> (string * Type.t) array
(** Returns the schema of the result as an array of column name and type pairs. *)
val fetch : t -> f:(Data_chunk.t option -> 'a) -> 'a
(** Fetches the next chunk of data from the result and applies the function f to it.
    Returns None when there are no more chunks. *)

val fetch_all : t -> int * (string * Packed_column.t) array
(** Fetches all data from the result and returns the row count and column data. *)

(** Returns a human-readable string representation of the result.
    Note: This function consumes the result, so calling it multiple times
    on the same result will produce different outputs as chunks are consumed. *)
val to_string_hum : ?bars:[ `Ascii | `Unicode ] -> t -> string

module Private : sig
  val to_struct : t -> Duckdb_stubs.Result.t Ctypes.structure Resource.t
end
