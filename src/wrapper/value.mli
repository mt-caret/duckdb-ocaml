open! Core

type t = Duckdb_stubs.Value.t

val create : 'a Type.Typed_non_null.t -> 'a -> t
val create_opt : 'a Type.Typed.t -> 'a option -> t
