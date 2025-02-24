(* [Date_] to avoid a naming collision with Core.Date *)
open! Core

type t = Duckdb_stubs.Date.t Ctypes.structure [@@deriving sexp_of, compare, quickcheck]

val to_days : t -> int32
val of_days : int32 -> t
val to_date : t -> Date.t option
val of_date : Date.t -> t option
