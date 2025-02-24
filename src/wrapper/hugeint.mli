open! Core

type t = Duckdb_stubs.Hugeint.t Ctypes.structure

val to_float : t -> float
val of_float : float -> t
