open! Core

(** Helper function to set DuckDB to single-threaded mode *)
val set_single_threaded : Duckdb.Connection.t -> unit

(** Helper function to set up single-threaded mode for all tests *)
val with_single_threaded_db : (Duckdb.Connection.t -> 'a) -> 'a

(** Helper function to test data operations on a simple table *)
val test_data_operations
  :  setup_sql:string
  -> query_sql:string
  -> f:(Duckdb.Result_.t -> 'a)
  -> 'a

(** Helper function to print result as a string with specified bar style *)
val print_result : ?bars:[ `Ascii | `Unicode ] -> Duckdb.Result_.t -> unit

(** Helper function to safely fetch a data chunk from a result *)
val fetch_chunk_exn : Duckdb.Result_.t -> Duckdb.Data_chunk.t

(** Helper function to get data from a chunk and free it *)
val with_chunk_data : Duckdb.Result_.t -> f:(Duckdb.Data_chunk.t -> 'a) -> 'a
