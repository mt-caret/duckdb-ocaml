open! Core
open! Ctypes

type t = Duckdb_stubs.Uhugeint.t Ctypes.structure

let lower t = getf t Duckdb_stubs.Uhugeint.lower
let upper t = getf t Duckdb_stubs.Uhugeint.upper

let create ~lower ~upper =
  let t = make Duckdb_stubs.Uhugeint.t in
  setf t Duckdb_stubs.Uhugeint.lower lower;
  setf t Duckdb_stubs.Uhugeint.upper upper;
  t
;;

let to_float = Duckdb_stubs.duckdb_uhugeint_to_double
let of_float = Duckdb_stubs.duckdb_double_to_uhugeint
