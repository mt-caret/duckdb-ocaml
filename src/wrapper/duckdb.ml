open! Core
open! Ctypes

module Database : sig
  type t

  val open_exn : string -> t
  val close : t -> unit
  val with_path : string -> f:(t -> 'a) -> 'a

  module Private : sig
    val to_ptr : t -> Duckdb_stubs.duckdb_database ptr
  end
end = struct
  type t = Duckdb_stubs.duckdb_database ptr

  let open_exn path =
    let t =
      allocate
        Duckdb_stubs.duckdb_database
        (from_voidp Duckdb_stubs.duckdb_database_struct null)
    in
    match Duckdb_stubs.duckdb_open path t with
    | DuckDBSuccess -> t
    | DuckDBError -> failwith "Failed to open database"
  ;;

  let close t = Duckdb_stubs.duckdb_close t
  let with_path path ~f = open_exn path |> Exn.protectx ~f ~finally:close

  module Private = struct
    let to_ptr = Fn.id
  end
end

module Connection : sig
  type t

  val connect_exn : Database.t -> t
  val disconnect : t -> unit
  val with_connection : Database.t -> f:(t -> 'a) -> 'a

  module Private : sig
    val to_ptr : t -> Duckdb_stubs.duckdb_connection ptr
  end
end = struct
  type t = Duckdb_stubs.duckdb_connection ptr

  let connect_exn db =
    let db = Database.Private.to_ptr db in
    let t =
      allocate
        Duckdb_stubs.duckdb_connection
        (from_voidp Duckdb_stubs.duckdb_connection_struct null)
    in
    match Duckdb_stubs.duckdb_connect !@db t with
    | DuckDBSuccess -> t
    | DuckDBError -> failwith "Failed to connect to database"
  ;;

  let disconnect t = Duckdb_stubs.duckdb_disconnect t
  let with_connection db ~f = connect_exn db |> Exn.protectx ~f ~finally:disconnect

  module Private = struct
    let to_ptr = Fn.id
  end
end

module Query : sig
  module Result : sig
    type t

    module Private : sig
      val to_struct : t -> Duckdb_stubs.duckdb_result structure
    end
  end

  val run : Connection.t -> string -> f:(Result.t -> 'a) -> 'a
  val run' : Connection.t -> string -> unit
end = struct
  module Result = struct
    type t = Duckdb_stubs.duckdb_result structure

    module Private = struct
      let to_struct = Fn.id
    end
  end

  let run conn query ~f =
    let conn = Connection.Private.to_ptr conn in
    let duckdb_result = make Duckdb_stubs.duckdb_result in
    match Duckdb_stubs.duckdb_query !@conn query (Some (addr duckdb_result)) with
    | DuckDBError ->
      Duckdb_stubs.duckdb_destroy_result (addr duckdb_result);
      failwith "Query failed"
    | DuckDBSuccess ->
      protectx duckdb_result ~f ~finally:(fun duckdb_result ->
        Duckdb_stubs.duckdb_destroy_result (addr duckdb_result))
  ;;

  let run' conn query = run conn query ~f:ignore
end

module Type = struct
  type t =
    | Boolean
    | Tiny_int
    | Small_int
    | Integer
    | Big_int
    | U_tiny_int
    | U_small_int
    | U_integer
    | U_big_int
    | Float
    | Double
    | Timestamp
    | Date
    | Time
    | Interval
    | Huge_int
    | Uhuge_int
    | Var_char
    | Blob
    | Decimal
    | Timestamp_s
    | Timestamp_ms
    | Timestamp_ns
    | Enum
    | List of t
    | Struct of (string * t) list
    | Map of t * t
    | Array of t * int
    | Uuid
    | Union of (string * t) list
    | Bit
    | Time_tz
    | Timestamp_tz
    | Var_int
  [@@deriving sexp]

  let with_logical_type logical_type ~f =
    let ptr = allocate Duckdb_stubs.duckdb_logical_type logical_type in
    match f !@ptr with
    | exception exn ->
      Duckdb_stubs.duckdb_destroy_logical_type ptr;
      raise exn
    | result ->
      Duckdb_stubs.duckdb_destroy_logical_type ptr;
      result
  ;;

  let rec of_logical_type_exn (logical_type : Duckdb_stubs.duckdb_logical_type) : t =
    match Duckdb_stubs.duckdb_get_type_id logical_type with
    | (DUCKDB_TYPE_INVALID | DUCKDB_TYPE_ANY | DUCKDB_TYPE_SQLNULL) as type_ ->
      let type_name = Duckdb_stubs.Variants_of_duckdb_type.to_name type_ in
      failwith [%string "Unsupported type: %{type_name}"]
    | DUCKDB_TYPE_BOOLEAN -> Boolean
    | DUCKDB_TYPE_TINYINT -> Tiny_int
    | DUCKDB_TYPE_SMALLINT -> Small_int
    | DUCKDB_TYPE_INTEGER -> Integer
    | DUCKDB_TYPE_BIGINT -> Big_int
    | DUCKDB_TYPE_UTINYINT -> U_tiny_int
    | DUCKDB_TYPE_USMALLINT -> U_small_int
    | DUCKDB_TYPE_UINTEGER -> U_integer
    | DUCKDB_TYPE_UBIGINT -> U_big_int
    | DUCKDB_TYPE_FLOAT -> Float
    | DUCKDB_TYPE_DOUBLE -> Double
    | DUCKDB_TYPE_TIMESTAMP -> Timestamp
    | DUCKDB_TYPE_DATE -> Date
    | DUCKDB_TYPE_TIME -> Time
    | DUCKDB_TYPE_INTERVAL -> Interval
    | DUCKDB_TYPE_HUGEINT -> Huge_int
    | DUCKDB_TYPE_UHUGEINT -> Uhuge_int
    | DUCKDB_TYPE_VARCHAR -> Var_char
    | DUCKDB_TYPE_BLOB -> Blob
    | DUCKDB_TYPE_DECIMAL -> Decimal
    | DUCKDB_TYPE_TIMESTAMP_S -> Timestamp_s
    | DUCKDB_TYPE_TIMESTAMP_MS -> Timestamp_ms
    | DUCKDB_TYPE_TIMESTAMP_NS -> Timestamp_ns
    | DUCKDB_TYPE_ENUM -> Enum
    | DUCKDB_TYPE_LIST ->
      Duckdb_stubs.duckdb_list_type_child_type logical_type
      |> with_logical_type ~f:(fun logical_type ->
        List (of_logical_type_exn logical_type))
    | DUCKDB_TYPE_STRUCT ->
      let child_count =
        Duckdb_stubs.duckdb_struct_type_child_count logical_type |> Unsigned.UInt64.to_int
      in
      let children =
        List.init child_count ~f:(fun i ->
          let i = Unsigned.UInt64.of_int i in
          let name = Duckdb_stubs.duckdb_struct_type_child_name logical_type i in
          Duckdb_stubs.duckdb_struct_type_child_type logical_type i
          |> with_logical_type ~f:(fun child_logical_type ->
            let child_type = of_logical_type_exn child_logical_type in
            name, child_type))
      in
      Struct children
    | DUCKDB_TYPE_MAP ->
      Duckdb_stubs.duckdb_map_type_key_type logical_type
      |> with_logical_type ~f:(fun key_logical_type ->
        let key = of_logical_type_exn key_logical_type in
        Duckdb_stubs.duckdb_map_type_value_type logical_type
        |> with_logical_type ~f:(fun value_logical_type ->
          let value = of_logical_type_exn value_logical_type in
          Map (key, value)))
    | DUCKDB_TYPE_ARRAY ->
      let array_size =
        Duckdb_stubs.duckdb_array_type_array_size logical_type |> Unsigned.UInt64.to_int
      in
      Duckdb_stubs.duckdb_array_type_child_type logical_type
      |> with_logical_type ~f:(fun child_logical_type ->
        let t = of_logical_type_exn child_logical_type in
        Array (t, array_size))
    | DUCKDB_TYPE_UUID -> Uuid
    | DUCKDB_TYPE_UNION ->
      let member_count =
        Duckdb_stubs.duckdb_union_type_member_count logical_type |> Unsigned.UInt64.to_int
      in
      let members =
        List.init member_count ~f:(fun i ->
          let i = Unsigned.UInt64.of_int i in
          let name = Duckdb_stubs.duckdb_union_type_member_name logical_type i in
          Duckdb_stubs.duckdb_union_type_member_type logical_type i
          |> with_logical_type ~f:(fun child_logical_type ->
            let child_type = of_logical_type_exn child_logical_type in
            name, child_type))
      in
      Union members
    | DUCKDB_TYPE_BIT -> Bit
    | DUCKDB_TYPE_TIME_TZ -> Time_tz
    | DUCKDB_TYPE_TIMESTAMP_TZ -> Timestamp_tz
    | DUCKDB_TYPE_VARINT -> Var_int
  ;;
end
