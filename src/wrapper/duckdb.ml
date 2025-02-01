open! Core
open! Ctypes

module Database : sig
  type t

  val open_exn : string -> t
  val close : t -> here:Source_code_position.t -> unit
  val with_path : string -> f:(t -> 'a) -> 'a

  module Private : sig
    val to_ptr : t -> Duckdb_stubs.duckdb_database ptr Resource.t
  end
end = struct
  type t = Duckdb_stubs.duckdb_database ptr Resource.t

  let open_exn path =
    let t =
      allocate
        Duckdb_stubs.duckdb_database
        (from_voidp Duckdb_stubs.duckdb_database_struct null)
    in
    match Duckdb_stubs.duckdb_open path t with
    | DuckDBSuccess ->
      Resource.create t ~name:"Duckdb.Database" ~free:Duckdb_stubs.duckdb_close
    | DuckDBError -> failwith "Failed to open database"
  ;;

  let close = Resource.free
  let with_path path ~f = open_exn path |> Exn.protectx ~f ~finally:(close ~here:[%here])

  module Private = struct
    let to_ptr = Fn.id
  end
end

module Connection : sig
  type t

  val connect_exn : Database.t -> t
  val disconnect : t -> here:Source_code_position.t -> unit
  val with_connection : Database.t -> f:(t -> 'a) -> 'a

  module Private : sig
    val to_ptr : t -> Duckdb_stubs.duckdb_connection ptr Resource.t
  end
end = struct
  type t = Duckdb_stubs.duckdb_connection ptr Resource.t

  let connect_exn db =
    let db = Database.Private.to_ptr db |> Resource.get_exn in
    let t =
      allocate
        Duckdb_stubs.duckdb_connection
        (from_voidp Duckdb_stubs.duckdb_connection_struct null)
    in
    match Duckdb_stubs.duckdb_connect !@db t with
    | DuckDBSuccess ->
      Resource.create t ~name:"Duckdb.Connection" ~free:Duckdb_stubs.duckdb_disconnect
    | DuckDBError -> failwith "Failed to connect to database"
  ;;

  let disconnect = Resource.free

  let with_connection db ~f =
    connect_exn db |> Exn.protectx ~f ~finally:(disconnect ~here:[%here])
  ;;

  module Private = struct
    let to_ptr = Fn.id
  end
end

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
  [@@deriving sexp, compare, equal]

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

  module Typed = struct
    type untyped = t

    (* TODO: add more types *)
    type _ t =
      | Boolean : bool t
      | Tiny_int : int t
      | Small_int : int t
      | Integer : int32 t
      | Big_int : int64 t
      | U_tiny_int : Unsigned.uint8 t
      | U_small_int : Unsigned.uint16 t
      | U_integer : Unsigned.uint32 t
      | U_big_int : Unsigned.uint64 t
      | Float : float t
      | Double : float t

    let to_untyped (type a) (t : a t) : untyped =
      match t with
      | Boolean -> Boolean
      | Tiny_int -> Tiny_int
      | Small_int -> Small_int
      | Integer -> Integer
      | Big_int -> Big_int
      | U_tiny_int -> U_tiny_int
      | U_small_int -> U_small_int
      | U_integer -> U_integer
      | U_big_int -> U_big_int
      | Float -> Float
      | Double -> Double
    ;;

    let to_c_type (type a) (t : a t) : a Ctypes.typ =
      match t with
      | Boolean -> bool
      | Tiny_int -> int8_t
      | Small_int -> int16_t
      | Integer -> int32_t
      | Big_int -> int64_t
      | U_tiny_int -> uint8_t
      | U_small_int -> uint16_t
      | U_integer -> uint32_t
      | U_big_int -> uint64_t
      | Float -> float
      | Double -> double
    ;;

    module List = struct
      type 'a type_ = 'a t

      type _ t =
        | [] : Nothing.t t
        | ( :: ) : 'a type_ * 'b t -> ('a * 'b) t

      let to_untyped =
        let rec go : type a. a t -> untyped list =
          fun t ->
          match t with
          | [] -> []
          | type_ :: t -> to_untyped type_ :: go t
        in
        go
      ;;
    end
  end
end

module Query : sig
  module Error : sig
    type t [@@deriving sexp]

    val message : t -> string
    val to_error : t -> Error.t
  end

  module Result : sig
    type t

    val column_count : t -> int
    val schema : t -> (string * Type.t) array

    module Private : sig
      val to_struct : t -> Duckdb_stubs.duckdb_result structure
    end
  end

  val run : Connection.t -> string -> f:(Result.t -> 'a) -> ('a, Error.t) result
  val run' : Connection.t -> string -> (unit, Error.t) result
  val run_exn : Connection.t -> string -> f:(Result.t -> 'a) -> 'a
  val run_exn' : Connection.t -> string -> unit

  module Prepared : sig
    module Parameters : sig
      type t =
        | [] : t
        | ( :: ) : ('a Type.Typed.t * 'a) * t -> t
    end

    type t

    val create : Connection.t -> string -> (t, string) result
    val destroy : t -> here:Source_code_position.t -> unit
    val bind : t -> Parameters.t -> (unit, string) result
    val clear_bindings_exn : t -> unit
    val run : t -> f:(Result.t -> 'a) -> ('a, Error.t) result
    val run' : t -> (unit, Error.t) result
    val run_exn : t -> f:(Result.t -> 'a) -> 'a
    val run_exn' : t -> unit
  end
end = struct
  module Error = struct
    type t =
      { kind : Duckdb_stubs.duckdb_error_type
      ; message : string
      }
    [@@deriving sexp, fields ~getters]

    let to_error t = Error.create_s [%sexp (t : t)]
  end

  module Result = struct
    type t =
      { result : Duckdb_stubs.duckdb_result structure
      ; schema : (string * Type.t) array
      }
    [@@deriving fields ~getters]

    let create result =
      let schema =
        Duckdb_stubs.duckdb_column_count (addr result)
        |> Unsigned.UInt64.to_int
        |> Array.init ~f:(fun i ->
          let i = Unsigned.UInt64.of_int i in
          let name = Duckdb_stubs.duckdb_column_name (addr result) i in
          let type_ =
            Duckdb_stubs.duckdb_column_logical_type (addr result) i
            |> with_logical_type ~f:Type.of_logical_type_exn
          in
          name, type_)
      in
      { result; schema }
    ;;

    let column_count t = Array.length t.schema

    module Private = struct
      let to_struct t = t.result
    end
  end

  let run conn query ~f =
    let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
    let duckdb_result = make Duckdb_stubs.duckdb_result in
    match Duckdb_stubs.duckdb_query !@conn query (Some (addr duckdb_result)) with
    | DuckDBError ->
      let error : Error.t =
        { kind = Duckdb_stubs.duckdb_result_error_type (addr duckdb_result)
        ; message = Duckdb_stubs.duckdb_result_error (addr duckdb_result)
        }
      in
      Duckdb_stubs.duckdb_destroy_result (addr duckdb_result);
      Error error
    | DuckDBSuccess ->
      let result = Result.create duckdb_result in
      protectx result ~f ~finally:(fun result ->
        Duckdb_stubs.duckdb_destroy_result (addr result.result))
      |> Ok
  ;;

  let run' conn query = run conn query ~f:ignore

  let run_exn conn query ~f =
    run conn query ~f |> Core.Result.map_error ~f:Error.to_error |> ok_exn
  ;;

  let run_exn' conn query =
    run' conn query |> Core.Result.map_error ~f:Error.to_error |> ok_exn
  ;;

  module Prepared = struct
    module Parameters = struct
      type t =
        | [] : t
        | ( :: ) : ('a Type.Typed.t * 'a) * t -> t

      let rec length = function
        | [] -> 0
        | _ :: t -> 1 + length t
      ;;
    end

    type t = Duckdb_stubs.duckdb_prepared_statement ptr Resource.t

    let bind (type a) t index (type_ : a Type.Typed.t) (value : a) =
      let index = Unsigned.UInt64.of_int index in
      match type_ with
      | Boolean -> Duckdb_stubs.duckdb_bind_boolean !@t index value
      | Tiny_int -> Duckdb_stubs.duckdb_bind_int8 !@t index value
      | Small_int -> Duckdb_stubs.duckdb_bind_int16 !@t index value
      | Integer -> Duckdb_stubs.duckdb_bind_int32 !@t index value
      | Big_int -> Duckdb_stubs.duckdb_bind_int64 !@t index value
      | U_tiny_int -> Duckdb_stubs.duckdb_bind_uint8 !@t index value
      | U_small_int -> Duckdb_stubs.duckdb_bind_uint16 !@t index value
      | U_integer -> Duckdb_stubs.duckdb_bind_uint32 !@t index value
      | U_big_int -> Duckdb_stubs.duckdb_bind_uint64 !@t index value
      | Float -> Duckdb_stubs.duckdb_bind_float !@t index value
      | Double -> Duckdb_stubs.duckdb_bind_double !@t index value
    ;;

    let destroy = Resource.free

    let create conn query =
      let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
      let t =
        allocate
          Duckdb_stubs.duckdb_prepared_statement
          (from_voidp Duckdb_stubs.duckdb_prepared_statement_struct null)
      in
      match Duckdb_stubs.duckdb_prepare !@conn query t with
      | DuckDBSuccess ->
        Ok
          (Resource.create
             t
             ~name:"Duckdb.Prepared"
             ~free:Duckdb_stubs.duckdb_destroy_prepare)
      | DuckDBError ->
        let error = Duckdb_stubs.duckdb_prepare_error !@t in
        Duckdb_stubs.duckdb_destroy_prepare t;
        Error (Option.value_exn error)
    ;;

    let bind =
      let rec go t (parameters : Parameters.t) index =
        match parameters with
        | [] -> Ok ()
        | (type_, value) :: parameters ->
          (* TODO: check that the type matches up? *)
          (match bind t index type_ value with
           | DuckDBSuccess -> go t parameters (index + 1)
           | DuckDBError ->
             let error =
               Duckdb_stubs.duckdb_prepare_error !@t |> Option.value ~default:""
             in
             Error [%string "Failed to bind parameter at index %{index#Int}: %{error}"])
      in
      fun t (parameters : Parameters.t) ->
        let t = Resource.get_exn t in
        let num_parameters = Duckdb_stubs.duckdb_nparams !@t |> Unsigned.UInt64.to_int in
        let actual_parameters = Parameters.length parameters in
        match num_parameters = actual_parameters with
        | false ->
          Error
            [%string
              "Expected %{num_parameters#Int} parameters, got %{actual_parameters#Int}"]
        | true -> go t parameters 1
    ;;

    let clear_bindings_exn t =
      let t = Resource.get_exn t in
      match Duckdb_stubs.duckdb_clear_bindings !@t with
      | DuckDBSuccess -> ()
      | DuckDBError ->
        let error = Duckdb_stubs.duckdb_prepare_error !@t in
        failwith (Option.value_exn error)
    ;;

    let run t ~f =
      let t = Resource.get_exn t in
      let duckdb_result = make Duckdb_stubs.duckdb_result in
      match Duckdb_stubs.duckdb_execute_prepared !@t (Some (addr duckdb_result)) with
      | DuckDBError ->
        let error : Error.t =
          { kind = Duckdb_stubs.duckdb_result_error_type (addr duckdb_result)
          ; message = Duckdb_stubs.duckdb_result_error (addr duckdb_result)
          }
        in
        Duckdb_stubs.duckdb_destroy_result (addr duckdb_result);
        Error error
      | DuckDBSuccess ->
        let result = Result.create duckdb_result in
        protectx result ~f ~finally:(fun result ->
          Duckdb_stubs.duckdb_destroy_result (addr result.result))
        |> Ok
    ;;

    let run' t = run t ~f:ignore
    let run_exn t ~f = run t ~f |> Core.Result.map_error ~f:Error.to_error |> ok_exn
    let run_exn' t = run' t |> Core.Result.map_error ~f:Error.to_error |> ok_exn
  end
end

module Data_chunk : sig
  type t

  val fetch : Query.Result.t -> f:(t option -> 'a) -> 'a
  val length : t -> int
  val schema : t -> (string * Type.t) array
  val get_exn : t -> 'a Type.Typed.t -> int -> 'a array
  val get_opt : t -> 'a Type.Typed.t -> int -> 'a option array

  module Private : sig
    val to_ptr : t -> Duckdb_stubs.duckdb_data_chunk ptr
  end
end = struct
  type t =
    { data_chunk : Duckdb_stubs.duckdb_data_chunk ptr
    ; length : int
    ; schema : (string * Type.t) array
    }
  [@@deriving fields ~getters]

  let fetch query_result ~f =
    match
      Duckdb_stubs.duckdb_fetch_chunk (Query.Result.Private.to_struct query_result)
    with
    | None -> f None
    | Some chunk ->
      let chunk = allocate Duckdb_stubs.duckdb_data_chunk chunk in
      let length =
        Duckdb_stubs.duckdb_data_chunk_get_size !@chunk |> Unsigned.UInt64.to_int
      in
      protect
        ~f:(fun () ->
          f
            (Some
               { data_chunk = chunk; length; schema = Query.Result.schema query_result }))
        ~finally:(fun () -> Duckdb_stubs.duckdb_destroy_data_chunk chunk)
  ;;

  let assert_all_valid vector ~len =
    match Duckdb_stubs.duckdb_vector_get_validity vector with
    | None -> ()
    | Some validity ->
      (match len with
       | 0 -> ()
       | len ->
         let all_bits_set =
           Unsigned.UInt64.sub
             (Unsigned.UInt64.shift_left Unsigned.UInt64.one len)
             Unsigned.UInt64.one
         in
         if not Unsigned.UInt64.(equal (logand all_bits_set !@validity) all_bits_set)
         then
           raise_s
             [%message
               "Not all rows are valid"
                 ~validity:(Unsigned.UInt64.to_int !@validity : int)])
  ;;

  let get_exn (type a) t (type_ : a Type.Typed.t) idx : a array =
    (* TODO: should check that type lines up with schema *)
    let vector =
      Duckdb_stubs.duckdb_data_chunk_get_vector
        !@(t.data_chunk)
        (Unsigned.UInt64.of_int idx)
    in
    assert_all_valid vector ~len:t.length;
    let data =
      Duckdb_stubs.duckdb_vector_get_data vector
      |> from_voidp (Type.Typed.to_c_type type_)
    in
    Array.init t.length ~f:(fun i -> !@(data +@ i))
  ;;

  let get_opt (type a) t (type_ : a Type.Typed.t) idx : a option array =
    (* TODO: should check that type lines up with schema *)
    let vector =
      Duckdb_stubs.duckdb_data_chunk_get_vector
        !@(t.data_chunk)
        (Unsigned.UInt64.of_int idx)
    in
    let validity = Duckdb_stubs.duckdb_vector_get_validity vector in
    let data =
      Duckdb_stubs.duckdb_vector_get_data vector
      |> from_voidp (Type.Typed.to_c_type type_)
    in
    match validity with
    | None -> Array.init t.length ~f:(fun i -> Some !@(data +@ i))
    | Some validity ->
      Array.init t.length ~f:(fun i ->
        match
          Duckdb_stubs.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i)
        with
        | true -> Some !@(data +@ i)
        | false -> None)
  ;;

  module Private = struct
    let to_ptr t = t.data_chunk
  end
end

module Heterogeneous_list = struct
  type 'a t =
    | [] : Nothing.t t
    | ( :: ) : 'a * 'b t -> ('a * 'b) t
end

module Appender : sig
  type t

  val create : Connection.t -> ?schema:string -> string -> t
  val close : t -> here:Source_code_position.t -> (unit, string) result
  val close_exn : t -> here:Source_code_position.t -> unit
  val flush : t -> here:Source_code_position.t -> (unit, string) result
  val flush_exn : t -> here:Source_code_position.t -> unit
  val column_types : t -> Type.t list

  val append
    :  t
    -> 'a Type.Typed.List.t
    -> 'a Heterogeneous_list.t list
    -> (unit, string) result

  val append_exn : t -> 'a Type.Typed.List.t -> 'a Heterogeneous_list.t list -> unit
end = struct
  type t = Duckdb_stubs.duckdb_appender ptr Resource.t

  let create conn ?schema table =
    let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
    let t =
      allocate
        Duckdb_stubs.duckdb_appender
        (from_voidp Duckdb_stubs.duckdb_appender_struct null)
    in
    match Duckdb_stubs.duckdb_appender_create !@conn schema table t with
    | DuckDBSuccess ->
      Resource.create t ~name:"Duckdb.Appender" ~free:(fun t ->
        match Duckdb_stubs.duckdb_appender_destroy t with
        | DuckDBSuccess | DuckDBError ->
          (* DuckDBError here means an error flushing the data, not destroying
             the appender, so it's fine to ignore. If the user cared about this,
             they should've explicitly flushed/closed it + checked the error. *)
          ())
    | DuckDBError -> failwith "Failed to create appender"
  ;;

  let to_result t (duckdb_state : Duckdb_stubs.duckdb_state) ~here =
    let t' = Resource.get_exn t in
    match duckdb_state with
    | DuckDBSuccess -> Ok ()
    | DuckDBError ->
      let error = Duckdb_stubs.duckdb_appender_error !@t' |> Option.value_exn ~here in
      Resource.free t ~here;
      Error error
  ;;

  let close t ~here =
    let t' = Resource.get_exn t in
    Duckdb_stubs.duckdb_appender_close !@t' |> to_result t ~here
  ;;

  let close_exn t ~here = close t ~here |> Result.ok_or_failwith

  let flush t ~here =
    let t' = Resource.get_exn t in
    Duckdb_stubs.duckdb_appender_flush !@t' |> to_result t ~here
  ;;

  let flush_exn t ~here = flush t ~here |> Result.ok_or_failwith

  let column_count t =
    let t = Resource.get_exn t in
    Duckdb_stubs.duckdb_appender_column_count !@t |> Unsigned.UInt64.to_int
  ;;

  let column_type t idx =
    let t = Resource.get_exn t in
    Duckdb_stubs.duckdb_appender_column_type !@t (Unsigned.UInt64.of_int idx)
    |> with_logical_type ~f:Type.of_logical_type_exn
  ;;

  let column_types t =
    let count = column_count t in
    List.init count ~f:(fun i -> column_type t i)
  ;;

  let append_value (type a) t (type_ : a Type.Typed.t) (value : a) =
    let t = Resource.get_exn t in
    match type_ with
    | Boolean -> Duckdb_stubs.duckdb_append_bool !@t value
    | Tiny_int -> Duckdb_stubs.duckdb_append_int8 !@t value
    | Small_int -> Duckdb_stubs.duckdb_append_int16 !@t value
    | Integer -> Duckdb_stubs.duckdb_append_int32 !@t value
    | Big_int -> Duckdb_stubs.duckdb_append_int64 !@t value
    | U_tiny_int -> Duckdb_stubs.duckdb_append_uint8 !@t value
    | U_small_int -> Duckdb_stubs.duckdb_append_uint16 !@t value
    | U_integer -> Duckdb_stubs.duckdb_append_uint32 !@t value
    | U_big_int -> Duckdb_stubs.duckdb_append_uint64 !@t value
    | Float -> Duckdb_stubs.duckdb_append_float !@t value
    | Double -> Duckdb_stubs.duckdb_append_double !@t value
  ;;

  let rec append_row
    : type a. t -> a Type.Typed.List.t -> a Heterogeneous_list.t -> (unit, string) result
    =
    fun (type a) t (types : a Type.Typed.List.t) (row : a Heterogeneous_list.t) ->
    match types, row with
    | [], [] ->
      let t' = Resource.get_exn t in
      Duckdb_stubs.duckdb_appender_end_row !@t' |> to_result t ~here:[%here]
    | type_ :: types, value :: row ->
      let%bind.Result () = append_value t type_ value |> to_result t ~here:[%here] in
      append_row t types row
  ;;

  let append (type a) t (types : a Type.Typed.List.t) (rows : a Heterogeneous_list.t list)
    =
    let expected_types = column_types t in
    let argument_types = Type.Typed.List.to_untyped types in
    if not ([%equal: Type.t list] expected_types argument_types)
    then
      raise_s
        [%message
          "Column types do not match"
            (expected_types : Type.t list)
            (argument_types : Type.t list)];
    List.fold_result rows ~init:() ~f:(fun () row -> append_row t types row)
  ;;

  let append_exn t types rows = append t types rows |> Result.ok_or_failwith
end
