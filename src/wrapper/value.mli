open! Core

type t = Duckdb_stubs.Value.t

val create : 'a Type.Typed.t -> 'a -> t
