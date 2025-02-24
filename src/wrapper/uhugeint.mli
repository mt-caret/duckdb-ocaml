open! Core

type t = Duckdb_stubs.Uhugeint.t Ctypes.structure

val to_float : t -> float
val of_float : float -> t
