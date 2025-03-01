open! Core

type t = Duckdb_stubs.Value.t

val create : 'a Type.Typed_non_null.t -> 'a -> t
