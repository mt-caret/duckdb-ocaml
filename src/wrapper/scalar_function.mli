open! Core

module Signature : sig
  type ('a, 'ret) t =
    | Returning : 'ret Type.Typed_non_null.t -> ('ret, 'ret) t
    | ( :: ) : 'a Type.Typed_non_null.t * ('b, 'ret) t -> ('a -> 'b, 'ret) t
end

type t

val create : string -> ('f, 'ret) Signature.t -> f:'f -> t
val register_exn : t -> Connection.t -> unit

module Expert : sig
  val create
    :  string
    -> Type.t list
    -> f:
         (Duckdb_stubs.Function_info.t
          -> Duckdb_stubs.Data_chunk.t
          -> Duckdb_stubs.Vector.t
          -> unit)
    -> t
end
