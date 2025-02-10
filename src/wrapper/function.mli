open! Core

module Scalar : sig
  type t

  val create
    :  string
    -> Type.t list
    -> f:
         (Duckdb_stubs.Function_info.t
          -> Duckdb_stubs.Data_chunk.t
          -> Duckdb_stubs.Vector.t
          -> unit)
    -> t

  val register_exn : t -> Connection.t -> unit
end
