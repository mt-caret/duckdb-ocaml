open! Core

(** Sets DuckDB to single-threaded mode to avoid segmentation faults

  This function configures DuckDB to use only a single thread for query execution,
  which prevents thread safety issues when DuckDB calls back into OCaml code.
  
  The segmentation fault occurs in the OCaml runtime's thread management system
  (specifically in caml_thread_leave_blocking_section) when DuckDB creates worker
  threads that call back into OCaml through registered scalar functions. By limiting
  DuckDB to a single thread, we prevent the creation of these worker threads.
*)
val with_single_threaded_db : (Duckdb.Connection.t -> 'a) -> 'a

val test_data_operations
  :  setup_sql:string
  -> query_sql:string
  -> f:(Duckdb.Result_.t -> 'a)
  -> 'a

val print_result : ?bars:[ `Ascii | `Unicode ] -> Duckdb.Result_.t -> unit
