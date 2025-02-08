open! Core
open! Ctypes

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
  | Decimal of
      { width : int
      ; scale : int
      }
  | Timestamp_s
  | Timestamp_ms
  | Timestamp_ns
  | Enum of string list
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

let with_logical_type logical_type ~f =
  allocate Duckdb_stubs.Logical_type.t logical_type
  |> protectx ~f:(fun ptr -> f !@ptr) ~finally:Duckdb_stubs.duckdb_destroy_logical_type
;;

let with_logical_types logical_types ~f =
  List.map logical_types ~f:(allocate Duckdb_stubs.Logical_type.t)
  |> protectx
       ~f:(fun ptrs -> List.map ptrs ~f:( !@ ) |> f)
       ~finally:(List.iter ~f:Duckdb_stubs.duckdb_destroy_logical_type)
;;

let rec of_logical_type_exn (logical_type : Duckdb_stubs.Logical_type.t) : t =
  match Duckdb_stubs.duckdb_get_type_id logical_type with
  | (DUCKDB_TYPE_INVALID | DUCKDB_TYPE_ANY | DUCKDB_TYPE_SQLNULL) as type_ ->
    let type_name = Duckdb_stubs.Type.Variants.to_name type_ in
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
  | DUCKDB_TYPE_DECIMAL ->
    let width = Duckdb_stubs.duckdb_decimal_width logical_type |> Unsigned.UInt8.to_int in
    let scale = Duckdb_stubs.duckdb_decimal_scale logical_type |> Unsigned.UInt8.to_int in
    Decimal { width; scale }
  | DUCKDB_TYPE_TIMESTAMP_S -> Timestamp_s
  | DUCKDB_TYPE_TIMESTAMP_MS -> Timestamp_ms
  | DUCKDB_TYPE_TIMESTAMP_NS -> Timestamp_ns
  | DUCKDB_TYPE_ENUM ->
    let dictionary_size =
      Duckdb_stubs.duckdb_enum_dictionary_size logical_type |> Unsigned.UInt32.to_int
    in
    let dictionary =
      List.init dictionary_size ~f:(fun i ->
        let i = Unsigned.UInt64.of_int i in
        Duckdb_stubs.duckdb_enum_dictionary_value logical_type i)
    in
    Enum dictionary
  | DUCKDB_TYPE_LIST ->
    Duckdb_stubs.duckdb_list_type_child_type logical_type
    |> with_logical_type ~f:(fun logical_type -> List (of_logical_type_exn logical_type))
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

let rec to_logical_type (t : t) : Duckdb_stubs.Logical_type.t =
  let (type_
        : [ `Primitive of Duckdb_stubs.Type.t
          | `List of t
          | `Array of t * int
          | `Map of t * t
          | `Enum of string list
          | `Decimal of int * int
          | `Struct of (string * t) list
          | `Union of (string * t) list
          ])
    =
    match t with
    | Boolean -> `Primitive DUCKDB_TYPE_BOOLEAN
    | Tiny_int -> `Primitive DUCKDB_TYPE_TINYINT
    | Small_int -> `Primitive DUCKDB_TYPE_SMALLINT
    | Integer -> `Primitive DUCKDB_TYPE_INTEGER
    | Big_int -> `Primitive DUCKDB_TYPE_BIGINT
    | U_tiny_int -> `Primitive DUCKDB_TYPE_UTINYINT
    | U_small_int -> `Primitive DUCKDB_TYPE_USMALLINT
    | U_integer -> `Primitive DUCKDB_TYPE_UINTEGER
    | U_big_int -> `Primitive DUCKDB_TYPE_UBIGINT
    | Float -> `Primitive DUCKDB_TYPE_FLOAT
    | Double -> `Primitive DUCKDB_TYPE_DOUBLE
    | Timestamp -> `Primitive DUCKDB_TYPE_TIMESTAMP
    | Date -> `Primitive DUCKDB_TYPE_DATE
    | Time -> `Primitive DUCKDB_TYPE_TIME
    | Interval -> `Primitive DUCKDB_TYPE_INTERVAL
    | Huge_int -> `Primitive DUCKDB_TYPE_HUGEINT
    | Uhuge_int -> `Primitive DUCKDB_TYPE_UHUGEINT
    | Var_char -> `Primitive DUCKDB_TYPE_VARCHAR
    | Blob -> `Primitive DUCKDB_TYPE_BLOB
    | Decimal { width; scale } -> `Decimal (width, scale)
    | Timestamp_s -> `Primitive DUCKDB_TYPE_TIMESTAMP_S
    | Timestamp_ms -> `Primitive DUCKDB_TYPE_TIMESTAMP_MS
    | Timestamp_ns -> `Primitive DUCKDB_TYPE_TIMESTAMP_NS
    | Enum dictionary -> `Enum dictionary
    | List child -> `List child
    | Struct children -> `Struct children
    | Map (key, value) -> `Map (key, value)
    | Array (child, size) -> `Array (child, size)
    | Uuid -> `Primitive DUCKDB_TYPE_UUID
    | Union members -> `Union members
    | Bit -> `Primitive DUCKDB_TYPE_BIT
    | Time_tz -> `Primitive DUCKDB_TYPE_TIME_TZ
    | Timestamp_tz -> `Primitive DUCKDB_TYPE_TIMESTAMP_TZ
    | Var_int -> `Primitive DUCKDB_TYPE_VARINT
  in
  match type_ with
  | `Primitive type_ -> Duckdb_stubs.duckdb_create_logical_type type_
  | `Decimal (width, scale) ->
    Duckdb_stubs.duckdb_create_decimal_type
      (Unsigned.UInt8.of_int width)
      (Unsigned.UInt8.of_int scale)
  | `Enum dictionary ->
    let dictionary_size = List.length dictionary in
    let dictionary_ptr = allocate_n string ~count:dictionary_size in
    List.iteri dictionary ~f:(fun i value -> dictionary_ptr +@ i <-@ value);
    Duckdb_stubs.duckdb_create_enum_type
      dictionary_ptr
      (Unsigned.UInt64.of_int dictionary_size)
  | `List child ->
    to_logical_type child |> with_logical_type ~f:Duckdb_stubs.duckdb_create_list_type
  | `Struct children ->
    let names, types = List.unzip children in
    List.map types ~f:to_logical_type
    |> with_logical_types ~f:(fun logical_types ->
      let names_ptr = allocate_n string ~count:(List.length names) in
      List.iteri names ~f:(fun i name -> names_ptr +@ i <-@ name);
      let types_ptr = allocate_n Duckdb_stubs.Logical_type.t ~count:(List.length types) in
      List.iteri logical_types ~f:(fun i logical_type -> types_ptr +@ i <-@ logical_type);
      Duckdb_stubs.duckdb_create_struct_type
        types_ptr
        names_ptr
        (Unsigned.UInt64.of_int (List.length children)))
  | `Union members ->
    let names, types = List.unzip members in
    List.map types ~f:to_logical_type
    |> with_logical_types ~f:(fun logical_types ->
      let names_ptr = allocate_n string ~count:(List.length names) in
      List.iteri names ~f:(fun i name -> names_ptr +@ i <-@ name);
      let types_ptr = allocate_n Duckdb_stubs.Logical_type.t ~count:(List.length types) in
      List.iteri logical_types ~f:(fun i logical_type -> types_ptr +@ i <-@ logical_type);
      Duckdb_stubs.duckdb_create_union_type
        types_ptr
        names_ptr
        (Unsigned.UInt64.of_int (List.length members)))
  | `Array (child, size) ->
    to_logical_type child
    |> with_logical_type ~f:(fun logical_type ->
      Duckdb_stubs.duckdb_create_array_type logical_type (Unsigned.UInt64.of_int size))
  | `Map (key, value) ->
    to_logical_type key
    |> with_logical_type ~f:(fun key_logical_type ->
      to_logical_type value
      |> with_logical_type ~f:(fun value_logical_type ->
        Duckdb_stubs.duckdb_create_map_type key_logical_type value_logical_type))
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
    | Timestamp : Duckdb_stubs.Timestamp.t Ctypes.structure t
    | Date : Duckdb_stubs.Date.t Ctypes.structure t
    | Time : Duckdb_stubs.Time.t Ctypes.structure t
    | Interval : Duckdb_stubs.Interval.t Ctypes.structure t
    | Huge_int : Duckdb_stubs.Hugeint.t Ctypes.structure t
    | Uhuge_int : Duckdb_stubs.Uhugeint.t Ctypes.structure t
    | Var_char : string t
    | Blob : string t

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
    | Timestamp -> Timestamp
    | Date -> Date
    | Time -> Time
    | Interval -> Interval
    | Huge_int -> Huge_int
    | Uhuge_int -> Uhuge_int
    | Var_char -> Var_char
    | Blob -> Blob
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
    | Timestamp -> Duckdb_stubs.Timestamp.t
    | Date -> Duckdb_stubs.Date.t
    | Time -> Duckdb_stubs.Time.t
    | Interval -> Duckdb_stubs.Interval.t
    | Huge_int -> Duckdb_stubs.Hugeint.t
    | Uhuge_int -> Duckdb_stubs.Uhugeint.t
    | Var_char -> string
    | Blob -> string
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

module Private = struct
  let with_logical_type = with_logical_type
end
