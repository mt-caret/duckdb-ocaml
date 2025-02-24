open! Core

type t = Duckdb_stubs.Time.t Ctypes.structure [@@deriving sexp_of, compare, quickcheck]

val to_micros_since_start_of_day : t -> int64
val of_micros_since_start_of_day : int64 -> t
val to_time_ofday : t -> Time_ns.Ofday.t option
val of_time_ofday : Time_ns.Ofday.t -> t
