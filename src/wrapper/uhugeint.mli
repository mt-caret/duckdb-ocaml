open! Core

type t = Duckdb_stubs.Uhugeint.t Ctypes.structure

val lower : t -> Unsigned.UInt64.t
val upper : t -> Unsigned.UInt64.t
val create : lower:Unsigned.UInt64.t -> upper:Unsigned.UInt64.t -> t
val to_float : t -> float
val of_float : float -> t
