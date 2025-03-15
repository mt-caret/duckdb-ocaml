open! Core

val add
  :  string
  -> Type.t list
  -> bind:(Duckdb_stubs.Bind_info.t -> unit)
  -> init:(Duckdb_stubs.Init_info.t -> unit)
  -> f:(Duckdb_stubs.Function_info.t -> Duckdb_stubs.Data_chunk.t -> unit)
  -> conn:Connection.t
  -> unit
