open! Core

val add
  :  Database.t
  -> f:(Duckdb_stubs.Replacement_scan.Info.t -> table_name:string -> unit)
  -> unit
