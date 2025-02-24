open! Core

type t = Duckdb_stubs.Interval.t Ctypes.structure
[@@deriving sexp_of, compare, quickcheck]

val months : t -> int32
val days : t -> int32
val micros : t -> int64
val create : months:int32 -> days:int32 -> micros:int64 -> t
val normalize : t -> t
val to_span : t -> Time_ns.Span.t option
val of_span : Time_ns.Span.t -> t option
