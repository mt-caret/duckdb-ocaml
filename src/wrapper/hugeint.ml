open! Core
open! Ctypes

type t = Duckdb_stubs.Hugeint.t Ctypes.structure

let to_float = Duckdb_stubs.duckdb_hugeint_to_double
let of_float = Duckdb_stubs.duckdb_double_to_hugeint
