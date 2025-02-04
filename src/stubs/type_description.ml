open! Core
open Ctypes

module Types (F : TYPE) = struct
  open! F

  module State = struct
    (* {[
      //! An enum over the returned state of different functions.
      typedef enum duckdb_state { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;
    ]} *)
    type t =
      | DuckDBSuccess
      | DuckDBError
    [@@deriving sexp, variants, enumerate]

    let t =
      List.map all ~f:(fun t -> t, constant (Variants.to_name t) int64_t)
      |> enum "duckdb_state" ~typedef:true
    ;;
  end

  module Internal_ptr_struct (T : sig
      val name : string
    end) : sig
    type t_struct
    type t = t_struct structure ptr

    val t_struct : t_struct structure typ
    val t : t typ
  end = struct
    type t_struct
    type t = t_struct structure ptr

    let t_struct = structure [%string "_%{T.name}"]
    let (_ : _ field) = field t_struct "internal_ptr" (ptr void)
    let () = seal t_struct
    let t = typedef (ptr t_struct) T.name
  end

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
  module Database = Internal_ptr_struct (struct
      let name = "duckdb_database"
    end)

  (* {[
    //! A connection to a duckdb database. Must be closed with `duckdb_disconnect`.
    typedef struct _duckdb_connection {
      void *internal_ptr;
    } * duckdb_connection;
  ]} *)
  module Connection = Internal_ptr_struct (struct
      let name = "duckdb_connection"
    end)

  (* {[
    //! A prepared statement is a parameterized query that allows you to bind parameters to it.
    //! Must be destroyed with `duckdb_destroy_prepare`.
    typedef struct _duckdb_prepared_statement {
      void *internal_ptr;
    } * duckdb_prepared_statement;
  ]} *)
  module Prepared_statement = Internal_ptr_struct (struct
      let name = "duckdb_prepared_statement"
    end)

  (* {[
    //! The appender enables fast data loading into DuckDB.
    //! Must be destroyed with `duckdb_appender_destroy`.
    typedef struct _duckdb_appender {
      void *internal_ptr;
    } * duckdb_appender;
  ]} *)
  module Appender = Internal_ptr_struct (struct
      let name = "duckdb_appender"
    end)

  (* {[
    //! DuckDB's index type.
    typedef uint64_t idx_t;
  ]} *)
  let idx_t = typedef uint64_t "idx_t"

  module Type = struct
    (* {[
      // WARNING: the numbers of these enums should not be changed, as changing the numbers breaks ABI compatibility
      // Always add enums at the END of the enum
      //! An enum over DuckDB's internal types.
      typedef enum DUCKDB_TYPE {
        DUCKDB_TYPE_INVALID = 0,
        // bool
        DUCKDB_TYPE_BOOLEAN = 1,
        // int8_t
        DUCKDB_TYPE_TINYINT = 2,
        // int16_t
        DUCKDB_TYPE_SMALLINT = 3,
        // int32_t
        DUCKDB_TYPE_INTEGER = 4,
        // int64_t
        DUCKDB_TYPE_BIGINT = 5,
        // uint8_t
        DUCKDB_TYPE_UTINYINT = 6,
        // uint16_t
        DUCKDB_TYPE_USMALLINT = 7,
        // uint32_t
        DUCKDB_TYPE_UINTEGER = 8,
        // uint64_t
        DUCKDB_TYPE_UBIGINT = 9,
        // float
        DUCKDB_TYPE_FLOAT = 10,
        // double
        DUCKDB_TYPE_DOUBLE = 11,
        // duckdb_timestamp, in microseconds
        DUCKDB_TYPE_TIMESTAMP = 12,
        // duckdb_date
        DUCKDB_TYPE_DATE = 13,
        // duckdb_time
        DUCKDB_TYPE_TIME = 14,
        // duckdb_interval
        DUCKDB_TYPE_INTERVAL = 15,
        // duckdb_hugeint
        DUCKDB_TYPE_HUGEINT = 16,
        // duckdb_uhugeint
        DUCKDB_TYPE_UHUGEINT = 32,
        // const char*
        DUCKDB_TYPE_VARCHAR = 17,
        // duckdb_blob
        DUCKDB_TYPE_BLOB = 18,
        // decimal
        DUCKDB_TYPE_DECIMAL = 19,
        // duckdb_timestamp, in seconds
        DUCKDB_TYPE_TIMESTAMP_S = 20,
        // duckdb_timestamp, in milliseconds
        DUCKDB_TYPE_TIMESTAMP_MS = 21,
        // duckdb_timestamp, in nanoseconds
        DUCKDB_TYPE_TIMESTAMP_NS = 22,
        // enum type, only useful as logical type
        DUCKDB_TYPE_ENUM = 23,
        // list type, only useful as logical type
        DUCKDB_TYPE_LIST = 24,
        // struct type, only useful as logical type
        DUCKDB_TYPE_STRUCT = 25,
        // map type, only useful as logical type
        DUCKDB_TYPE_MAP = 26,
        // duckdb_array, only useful as logical type
        DUCKDB_TYPE_ARRAY = 33,
        // duckdb_hugeint
        DUCKDB_TYPE_UUID = 27,
        // union type, only useful as logical type
        DUCKDB_TYPE_UNION = 28,
        // duckdb_bit
        DUCKDB_TYPE_BIT = 29,
        // duckdb_time_tz
        DUCKDB_TYPE_TIME_TZ = 30,
        // duckdb_timestamp
        DUCKDB_TYPE_TIMESTAMP_TZ = 31,
        // ANY type
        DUCKDB_TYPE_ANY = 34,
        // duckdb_varint
        DUCKDB_TYPE_VARINT = 35,
        // SQLNULL type
      DUCKDB_TYPE_SQLNULL = 36,
    } duckdb_type;
    ]}*)

    type t =
      | DUCKDB_TYPE_INVALID
      | DUCKDB_TYPE_BOOLEAN
      | DUCKDB_TYPE_TINYINT
      | DUCKDB_TYPE_SMALLINT
      | DUCKDB_TYPE_INTEGER
      | DUCKDB_TYPE_BIGINT
      | DUCKDB_TYPE_UTINYINT
      | DUCKDB_TYPE_USMALLINT
      | DUCKDB_TYPE_UINTEGER
      | DUCKDB_TYPE_UBIGINT
      | DUCKDB_TYPE_FLOAT
      | DUCKDB_TYPE_DOUBLE
      | DUCKDB_TYPE_TIMESTAMP
      | DUCKDB_TYPE_DATE
      | DUCKDB_TYPE_TIME
      | DUCKDB_TYPE_INTERVAL
      | DUCKDB_TYPE_HUGEINT
      | DUCKDB_TYPE_UHUGEINT
      | DUCKDB_TYPE_VARCHAR
      | DUCKDB_TYPE_BLOB
      | DUCKDB_TYPE_DECIMAL
      | DUCKDB_TYPE_TIMESTAMP_S
      | DUCKDB_TYPE_TIMESTAMP_MS
      | DUCKDB_TYPE_TIMESTAMP_NS
      | DUCKDB_TYPE_ENUM
      | DUCKDB_TYPE_LIST
      | DUCKDB_TYPE_STRUCT
      | DUCKDB_TYPE_MAP
      | DUCKDB_TYPE_ARRAY
      | DUCKDB_TYPE_UUID
      | DUCKDB_TYPE_UNION
      | DUCKDB_TYPE_BIT
      | DUCKDB_TYPE_TIME_TZ
      | DUCKDB_TYPE_TIMESTAMP_TZ
      | DUCKDB_TYPE_ANY
      | DUCKDB_TYPE_VARINT
      | DUCKDB_TYPE_SQLNULL
    [@@deriving sexp, variants, enumerate]

    let t =
      List.map all ~f:(fun t -> t, constant (Variants.to_name t) int64_t)
      |> enum "duckdb_type" ~typedef:true
    ;;
  end

  module Error_type = struct
    (* {[
      //! An enum over DuckDB's different result types.
      typedef enum duckdb_error_type {
        DUCKDB_ERROR_INVALID = 0,
        DUCKDB_ERROR_OUT_OF_RANGE = 1,
        DUCKDB_ERROR_CONVERSION = 2,
        DUCKDB_ERROR_UNKNOWN_TYPE = 3,
        DUCKDB_ERROR_DECIMAL = 4,
        DUCKDB_ERROR_MISMATCH_TYPE = 5,
        DUCKDB_ERROR_DIVIDE_BY_ZERO = 6,
        DUCKDB_ERROR_OBJECT_SIZE = 7,
        DUCKDB_ERROR_INVALID_TYPE = 8,
        DUCKDB_ERROR_SERIALIZATION = 9,
        DUCKDB_ERROR_TRANSACTION = 10,
        DUCKDB_ERROR_NOT_IMPLEMENTED = 11,
        DUCKDB_ERROR_EXPRESSION = 12,
        DUCKDB_ERROR_CATALOG = 13,
        DUCKDB_ERROR_PARSER = 14,
        DUCKDB_ERROR_PLANNER = 15,
        DUCKDB_ERROR_SCHEDULER = 16,
        DUCKDB_ERROR_EXECUTOR = 17,
        DUCKDB_ERROR_CONSTRAINT = 18,
        DUCKDB_ERROR_INDEX = 19,
        DUCKDB_ERROR_STAT = 20,
        DUCKDB_ERROR_CONNECTION = 21,
        DUCKDB_ERROR_SYNTAX = 22,
        DUCKDB_ERROR_SETTINGS = 23,
        DUCKDB_ERROR_BINDER = 24,
        DUCKDB_ERROR_NETWORK = 25,
        DUCKDB_ERROR_OPTIMIZER = 26,
        DUCKDB_ERROR_NULL_POINTER = 27,
        DUCKDB_ERROR_IO = 28,
        DUCKDB_ERROR_INTERRUPT = 29,
        DUCKDB_ERROR_FATAL = 30,
        DUCKDB_ERROR_INTERNAL = 31,
        DUCKDB_ERROR_INVALID_INPUT = 32,
        DUCKDB_ERROR_OUT_OF_MEMORY = 33,
        DUCKDB_ERROR_PERMISSION = 34,
        DUCKDB_ERROR_PARAMETER_NOT_RESOLVED = 35,
        DUCKDB_ERROR_PARAMETER_NOT_ALLOWED = 36,
        DUCKDB_ERROR_DEPENDENCY = 37,
        DUCKDB_ERROR_HTTP = 38,
        DUCKDB_ERROR_MISSING_EXTENSION = 39,
        DUCKDB_ERROR_AUTOLOAD = 40,
        DUCKDB_ERROR_SEQUENCE = 41,
        DUCKDB_INVALID_CONFIGURATION = 42
      } duckdb_error_type;
    ]} *)

    type t =
      | DUCKDB_ERROR_INVALID
      | DUCKDB_ERROR_OUT_OF_RANGE
      | DUCKDB_ERROR_CONVERSION
      | DUCKDB_ERROR_UNKNOWN_TYPE
      | DUCKDB_ERROR_DECIMAL
      | DUCKDB_ERROR_MISMATCH_TYPE
      | DUCKDB_ERROR_DIVIDE_BY_ZERO
      | DUCKDB_ERROR_OBJECT_SIZE
      | DUCKDB_ERROR_INVALID_TYPE
      | DUCKDB_ERROR_SERIALIZATION
      | DUCKDB_ERROR_TRANSACTION
      | DUCKDB_ERROR_NOT_IMPLEMENTED
      | DUCKDB_ERROR_EXPRESSION
      | DUCKDB_ERROR_CATALOG
      | DUCKDB_ERROR_PARSER
      | DUCKDB_ERROR_PLANNER
      | DUCKDB_ERROR_SCHEDULER
      | DUCKDB_ERROR_EXECUTOR
      | DUCKDB_ERROR_CONSTRAINT
      | DUCKDB_ERROR_INDEX
      | DUCKDB_ERROR_STAT
      | DUCKDB_ERROR_CONNECTION
      | DUCKDB_ERROR_SYNTAX
      | DUCKDB_ERROR_SETTINGS
      | DUCKDB_ERROR_BINDER
      | DUCKDB_ERROR_NETWORK
      | DUCKDB_ERROR_OPTIMIZER
      | DUCKDB_ERROR_NULL_POINTER
      | DUCKDB_ERROR_IO
      | DUCKDB_ERROR_INTERRUPT
      | DUCKDB_ERROR_FATAL
      | DUCKDB_ERROR_INTERNAL
      | DUCKDB_ERROR_INVALID_INPUT
      | DUCKDB_ERROR_OUT_OF_MEMORY
      | DUCKDB_ERROR_PERMISSION
      | DUCKDB_ERROR_PARAMETER_NOT_RESOLVED
      | DUCKDB_ERROR_PARAMETER_NOT_ALLOWED
      | DUCKDB_ERROR_DEPENDENCY
      | DUCKDB_ERROR_HTTP
      | DUCKDB_ERROR_MISSING_EXTENSION
      | DUCKDB_ERROR_AUTOLOAD
      | DUCKDB_ERROR_SEQUENCE
      | DUCKDB_INVALID_CONFIGURATION
    [@@deriving sexp, variants, enumerate]

    let t =
      List.map all ~f:(fun t -> t, constant (Variants.to_name t) int64_t)
      |> enum "duckdb_error_type" ~typedef:true
    ;;
  end

  module Column = struct
    type t

    let t : t structure typ =
      let struct_ = typedef (structure "_duckdb_column") "duckdb_column" in
      let (_ : _ field) = field struct_ "deprecated_data" (ptr void) in
      let (_ : _ field) = field struct_ "deprecated_nullmask" (ptr bool) in
      let (_ : _ field) = field struct_ "deprecated_type" Type.t in
      let (_ : _ field) = field struct_ "deprecated_name" (ptr char) in
      let (_ : _ field) = field struct_ "internal_data" (ptr void) in
      seal struct_;
      struct_
    ;;
  end

  module Result = struct
    type t

    let t : t structure typ =
      let struct_ = typedef (structure "_duckdb_result") "duckdb_result" in
      let (_ : _ field) = field struct_ "deprecated_column_count" idx_t in
      let (_ : _ field) = field struct_ "deprecated_row_count" idx_t in
      let (_ : _ field) = field struct_ "deprecated_rows_changed" idx_t in
      let (_ : _ field) = field struct_ "deprecated_columns" (ptr Column.t) in
      let (_ : _ field) = field struct_ "deprecated_error_message" (ptr char) in
      let (_ : _ field) = field struct_ "internal_data" (ptr void) in
      seal struct_;
      struct_
    ;;
  end

  (* {[
    //! Contains a data chunk from a duckdb_result.
    //! Must be destroyed with `duckdb_destroy_data_chunk`.
    typedef struct _duckdb_data_chunk {
      void *internal_ptr;
    } * duckdb_data_chunk;
  ]} *)
  module Data_chunk = Internal_ptr_struct (struct
      let name = "duckdb_data_chunk"
    end)

  (* {[
    //! A vector to a specified column in a data chunk. Lives as long as the
    //! data chunk lives, i.e., must not be destroyed.
    typedef struct _duckdb_vector {
      void *internal_ptr;
    } * duckdb_vector;
  ]} *)
  module Vector = Internal_ptr_struct (struct
      let name = "duckdb_vector"
    end)

  (* {[
    //! Holds an internal logical type.
    //! Must be destroyed with `duckdb_destroy_logical_type`.
    typedef struct _duckdb_logical_type {
      void *internal_ptr;
    } * duckdb_logical_type;
  ]} *)
  module Logical_type = Internal_ptr_struct (struct
      let name = "duckdb_logical_type"
    end)
end
