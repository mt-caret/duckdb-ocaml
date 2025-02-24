open! Core
open! Ctypes

type t = Duckdb_stubs.Hugeint.t Ctypes.structure

let lower t = getf t Duckdb_stubs.Hugeint.lower
let upper t = getf t Duckdb_stubs.Hugeint.upper

let create ~lower ~upper =
  let t = make Duckdb_stubs.Hugeint.t in
  setf t Duckdb_stubs.Hugeint.lower lower;
  setf t Duckdb_stubs.Hugeint.upper upper;
  t
;;

let to_float = Duckdb_stubs.duckdb_hugeint_to_double
let of_float = Duckdb_stubs.duckdb_double_to_hugeint
