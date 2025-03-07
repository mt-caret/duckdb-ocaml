open! Core

type t = Duckdb_stubs.Value.t

val create : 'a Type.Typed.t -> 'a option -> t
val create_non_null : 'a Type.Typed_non_null.t -> 'a -> t
