open Ctypes

module Types (F : TYPE) = struct
  open! F

  type duckdb_state =
    | DuckDBSuccess
    | DuckDBError
  [@@deriving sexp]

  let duckdb_success = constant "DuckDBSuccess" int64_t
  let duckdb_error = constant "DuckDBError" int64_t

  let duckdb_state =
    enum "duckdb_state" [ DuckDBSuccess, duckdb_success; DuckDBError, duckdb_error ]
  ;;

  (* {[
    typedef struct _duckdb_database {
      void *internal_ptr;
    } * duckdb_database;
  ]} *)
  type duckdb_database_struct

  let duckdb_database_struct : duckdb_database_struct structure typ =
    structure "_duckdb_database"
  ;;

  let (_ : _ field) = field duckdb_database_struct "internal_ptr" (ptr void)
  let () = seal duckdb_database_struct

  type duckdb_database = duckdb_database_struct structure ptr

  let duckdb_database : duckdb_database typ =
    typedef (ptr duckdb_database_struct) "duckdb_database"
  ;;
end
