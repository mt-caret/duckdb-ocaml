open Ctypes

module Types (F : TYPE) = struct
  open! F

  (* {[
    //! An enum over the returned state of different functions.
    typedef enum duckdb_state { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;
  ]} *)
  type duckdb_state =
    | DuckDBSuccess
    | DuckDBError
  [@@deriving sexp]

  let duckdb_success = constant "DuckDBSuccess" int64_t
  let duckdb_error = constant "DuckDBError" int64_t

  let duckdb_state =
    enum
      "duckdb_state"
      ~typedef:true
      [ DuckDBSuccess, duckdb_success; DuckDBError, duckdb_error ]
  ;;

  (* TODO: I feel like we don't need to expose the struct type here. *)
  let duckdb_struct_type_and_typedef name =
    let struct_ = structure [%string "_%{name}"] in
    let (_ : _ field) = field struct_ "internal_ptr" (ptr void) in
    seal struct_;
    struct_, typedef (ptr struct_) name
  ;;

  (* {[
    //! A database object. Should be closed with `duckdb_close`.
    typedef struct _duckdb_database {
      void *internal_ptr;
    } * duckdb_database;
  ]} *)
  type duckdb_database_struct
  type duckdb_database = duckdb_database_struct structure ptr

  let (duckdb_database_struct, duckdb_database)
    : duckdb_database_struct structure typ * duckdb_database typ
    =
    duckdb_struct_type_and_typedef "duckdb_database"
  ;;

  (* {[
    //! A connection to a duckdb database. Must be closed with `duckdb_disconnect`.
    typedef struct _duckdb_connection {
      void *internal_ptr;
    } * duckdb_connection;
  ]}*)
  type duckdb_connection_struct
  type duckdb_connection = duckdb_connection_struct structure ptr

  let (duckdb_connection_struct, duckdb_connection)
    : duckdb_connection_struct structure typ * duckdb_connection typ
    =
    duckdb_struct_type_and_typedef "duckdb_connection"
  ;;
end
