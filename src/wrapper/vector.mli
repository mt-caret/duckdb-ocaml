open! Core

type t = Duckdb_stubs.Vector.t

val assert_all_valid : t -> length:int -> unit
val to_array_exn : t -> 'a Type.Typed.t -> length:int -> 'a array
val to_option_array : t -> 'a Type.Typed.t -> length:int -> 'a option array
val set_array : t -> 'a Type.Typed.t -> 'a array -> unit
