open! Core

type t = Duckdb_stubs.Hugeint.t Ctypes.structure

val lower : t -> Unsigned.UInt64.t
val upper : t -> int64
val create : lower:Unsigned.UInt64.t -> upper:int64 -> t
val to_float : t -> float
val of_float : float -> t
