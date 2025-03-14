open! Core

(* Add a function to set DuckDB to single-threaded mode *)
let set_single_threaded conn = Duckdb.Query.run_exn' conn "SET threads TO 1"
