open! Core

module Signature : sig
  type ('a, 'ret) t =
    | Returning : 'ret Type.Typed_non_null.t -> ('ret, 'ret) t
    | ( :: ) : 'a Type.Typed_non_null.t * ('b, 'ret) t -> ('a -> 'b, 'ret) t
end

val create_exn : string -> ('f, 'ret) Signature.t -> f:'f -> conn:Connection.t -> unit

module Expert : sig
  val create_exn
    :  string
    -> Type.t list
    -> f:
         (Duckdb_stubs.Function_info.t
          -> Duckdb_stubs.Data_chunk.t
          -> Duckdb_stubs.Vector.t
          -> unit)
    -> conn:Connection.t
    -> unit
end
