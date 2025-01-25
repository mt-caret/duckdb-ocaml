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

  (* {[
    //! DuckDB's index type.
    typedef uint64_t idx_t;
  ]} *)
  let idx_t = typedef uint64_t "idx_t"

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

  type duckdb_type =
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
  [@@deriving sexp, variants]

  let duckdb_type_invalid = constant "DUCKDB_TYPE_INVALID" int64_t
  let duckdb_type_boolean = constant "DUCKDB_TYPE_BOOLEAN" int64_t
  let duckdb_type_tinyint = constant "DUCKDB_TYPE_TINYINT" int64_t
  let duckdb_type_smallint = constant "DUCKDB_TYPE_SMALLINT" int64_t
  let duckdb_type_integer = constant "DUCKDB_TYPE_INTEGER" int64_t
  let duckdb_type_bigint = constant "DUCKDB_TYPE_BIGINT" int64_t
  let duckdb_type_utinyint = constant "DUCKDB_TYPE_UTINYINT" int64_t
  let duckdb_type_usmallint = constant "DUCKDB_TYPE_USMALLINT" int64_t
  let duckdb_type_uinteger = constant "DUCKDB_TYPE_UINTEGER" int64_t
  let duckdb_type_ubigint = constant "DUCKDB_TYPE_UBIGINT" int64_t
  let duckdb_type_float = constant "DUCKDB_TYPE_FLOAT" int64_t
  let duckdb_type_double = constant "DUCKDB_TYPE_DOUBLE" int64_t
  let duckdb_type_timestamp = constant "DUCKDB_TYPE_TIMESTAMP" int64_t
  let duckdb_type_date = constant "DUCKDB_TYPE_DATE" int64_t
  let duckdb_type_time = constant "DUCKDB_TYPE_TIME" int64_t
  let duckdb_type_interval = constant "DUCKDB_TYPE_INTERVAL" int64_t
  let duckdb_type_hugeint = constant "DUCKDB_TYPE_HUGEINT" int64_t
  let duckdb_type_uhugeint = constant "DUCKDB_TYPE_UHUGEINT" int64_t
  let duckdb_type_varchar = constant "DUCKDB_TYPE_VARCHAR" int64_t
  let duckdb_type_blob = constant "DUCKDB_TYPE_BLOB" int64_t
  let duckdb_type_decimal = constant "DUCKDB_TYPE_DECIMAL" int64_t
  let duckdb_type_timestamp_s = constant "DUCKDB_TYPE_TIMESTAMP_S" int64_t
  let duckdb_type_timestamp_ms = constant "DUCKDB_TYPE_TIMESTAMP_MS" int64_t
  let duckdb_type_timestamp_ns = constant "DUCKDB_TYPE_TIMESTAMP_NS" int64_t
  let duckdb_type_enum = constant "DUCKDB_TYPE_ENUM" int64_t
  let duckdb_type_list = constant "DUCKDB_TYPE_LIST" int64_t
  let duckdb_type_struct = constant "DUCKDB_TYPE_STRUCT" int64_t
  let duckdb_type_map = constant "DUCKDB_TYPE_MAP" int64_t
  let duckdb_type_array = constant "DUCKDB_TYPE_ARRAY" int64_t
  let duckdb_type_uuid = constant "DUCKDB_TYPE_UUID" int64_t
  let duckdb_type_union = constant "DUCKDB_TYPE_UNION" int64_t
  let duckdb_type_bit = constant "DUCKDB_TYPE_BIT" int64_t
  let duckdb_type_time_tz = constant "DUCKDB_TYPE_TIME_TZ" int64_t
  let duckdb_type_timestamp_tz = constant "DUCKDB_TYPE_TIMESTAMP_TZ" int64_t
  let duckdb_type_any = constant "DUCKDB_TYPE_ANY" int64_t
  let duckdb_type_varint = constant "DUCKDB_TYPE_VARINT" int64_t
  let duckdb_type_sqlnull = constant "DUCKDB_TYPE_SQLNULL" int64_t

  let duckdb_type =
    enum
      "duckdb_type"
      ~typedef:true
      [ DUCKDB_TYPE_INVALID, duckdb_type_invalid
      ; DUCKDB_TYPE_BOOLEAN, duckdb_type_boolean
      ; DUCKDB_TYPE_TINYINT, duckdb_type_tinyint
      ; DUCKDB_TYPE_SMALLINT, duckdb_type_smallint
      ; DUCKDB_TYPE_INTEGER, duckdb_type_integer
      ; DUCKDB_TYPE_BIGINT, duckdb_type_bigint
      ; DUCKDB_TYPE_UTINYINT, duckdb_type_utinyint
      ; DUCKDB_TYPE_USMALLINT, duckdb_type_usmallint
      ; DUCKDB_TYPE_UINTEGER, duckdb_type_uinteger
      ; DUCKDB_TYPE_UBIGINT, duckdb_type_ubigint
      ; DUCKDB_TYPE_FLOAT, duckdb_type_float
      ; DUCKDB_TYPE_DOUBLE, duckdb_type_double
      ; DUCKDB_TYPE_TIMESTAMP, duckdb_type_timestamp
      ; DUCKDB_TYPE_DATE, duckdb_type_date
      ; DUCKDB_TYPE_TIME, duckdb_type_time
      ; DUCKDB_TYPE_INTERVAL, duckdb_type_interval
      ; DUCKDB_TYPE_HUGEINT, duckdb_type_hugeint
      ; DUCKDB_TYPE_UHUGEINT, duckdb_type_uhugeint
      ; DUCKDB_TYPE_VARCHAR, duckdb_type_varchar
      ; DUCKDB_TYPE_BLOB, duckdb_type_blob
      ; DUCKDB_TYPE_DECIMAL, duckdb_type_decimal
      ; DUCKDB_TYPE_TIMESTAMP_S, duckdb_type_timestamp_s
      ; DUCKDB_TYPE_TIMESTAMP_MS, duckdb_type_timestamp_ms
      ; DUCKDB_TYPE_TIMESTAMP_NS, duckdb_type_timestamp_ns
      ; DUCKDB_TYPE_ENUM, duckdb_type_enum
      ; DUCKDB_TYPE_LIST, duckdb_type_list
      ; DUCKDB_TYPE_STRUCT, duckdb_type_struct
      ; DUCKDB_TYPE_MAP, duckdb_type_map
      ; DUCKDB_TYPE_ARRAY, duckdb_type_array
      ; DUCKDB_TYPE_UUID, duckdb_type_uuid
      ; DUCKDB_TYPE_UNION, duckdb_type_union
      ; DUCKDB_TYPE_BIT, duckdb_type_bit
      ; DUCKDB_TYPE_TIME_TZ, duckdb_type_time_tz
      ; DUCKDB_TYPE_TIMESTAMP_TZ, duckdb_type_timestamp_tz
      ; DUCKDB_TYPE_ANY, duckdb_type_any
      ; DUCKDB_TYPE_VARINT, duckdb_type_varint
      ; DUCKDB_TYPE_SQLNULL, duckdb_type_sqlnull
      ]
  ;;

  type duckdb_column

  let duckdb_column : duckdb_column structure typ =
    let struct_ = typedef (structure "_duckdb_column") "duckdb_column" in
    let (_ : _ field) = field struct_ "deprecated_data" (ptr void) in
    let (_ : _ field) = field struct_ "deprecated_nullmask" (ptr bool) in
    let (_ : _ field) = field struct_ "deprecated_type" duckdb_type in
    let (_ : _ field) = field struct_ "deprecated_name" (ptr char) in
    let (_ : _ field) = field struct_ "internal_data" (ptr void) in
    seal struct_;
    struct_
  ;;

  type duckdb_result

  let duckdb_result : duckdb_result structure typ =
    let struct_ = typedef (structure "_duckdb_result") "duckdb_result" in
    let (_ : _ field) = field struct_ "deprecated_column_count" idx_t in
    let (_ : _ field) = field struct_ "deprecated_row_count" idx_t in
    let (_ : _ field) = field struct_ "deprecated_rows_changed" idx_t in
    let (_ : _ field) = field struct_ "deprecated_columns" (ptr duckdb_column) in
    let (_ : _ field) = field struct_ "deprecated_error_message" (ptr char) in
    let (_ : _ field) = field struct_ "internal_data" (ptr void) in
    seal struct_;
    struct_
  ;;

  (* {[
    //! Contains a data chunk from a duckdb_result.
    //! Must be destroyed with `duckdb_destroy_data_chunk`.
    typedef struct _duckdb_data_chunk {
      void *internal_ptr;
    } * duckdb_data_chunk;
  ]} *)
  type duckdb_data_chunk_struct
  type duckdb_data_chunk = duckdb_data_chunk_struct structure ptr

  let (duckdb_data_chunk_struct, duckdb_data_chunk)
    : duckdb_data_chunk_struct structure typ * duckdb_data_chunk typ
    =
    duckdb_struct_type_and_typedef "duckdb_data_chunk"
  ;;

  (* {[
    //! A vector to a specified column in a data chunk. Lives as long as the
    //! data chunk lives, i.e., must not be destroyed.
    typedef struct _duckdb_vector {
      void *internal_ptr;
    } * duckdb_vector;
  ]}*)
  type duckdb_vector_struct
  type duckdb_vector = duckdb_vector_struct structure ptr

  let (duckdb_vector_struct, duckdb_vector)
    : duckdb_vector_struct structure typ * duckdb_vector typ
    =
    duckdb_struct_type_and_typedef "duckdb_vector"
  ;;

  (* {[
    //! Holds an internal logical type.
    //! Must be destroyed with `duckdb_destroy_logical_type`.
    typedef struct _duckdb_logical_type {
      void *internal_ptr;
    } * duckdb_logical_type;
  ]} *)
  type duckdb_logical_type_struct
  type duckdb_logical_type = duckdb_logical_type_struct structure ptr

  let (duckdb_logical_type_struct, duckdb_logical_type)
    : duckdb_logical_type_struct structure typ * duckdb_logical_type typ
    =
    duckdb_struct_type_and_typedef "duckdb_logical_type"
  ;;
end
