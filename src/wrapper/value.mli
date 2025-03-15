open! Core

type t = Duckdb_stubs.Value.t

val create : 'a Type.Typed.t -> 'a option -> t
val create_non_null : 'a Type.Typed_non_null.t -> 'a -> t
val get : 'a Type.Typed.t -> t -> 'a
val get_non_null : 'a Type.Typed_non_null.t -> t -> 'a
