open! Core
open! Ctypes

type t = Duckdb_stubs.Uhugeint.t Ctypes.structure

let to_float = Duckdb_stubs.duckdb_uhugeint_to_double
let of_float = Duckdb_stubs.duckdb_double_to_uhugeint
