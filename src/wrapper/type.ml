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

let with_logical_type logical_type ~f =
  let ptr = allocate Duckdb_stubs.Logical_type.t logical_type in
  match f !@ptr with
  | exception exn ->
    Duckdb_stubs.duckdb_destroy_logical_type ptr;
    raise exn
  | result ->
    Duckdb_stubs.duckdb_destroy_logical_type ptr;
    result
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
  | DUCKDB_TYPE_DECIMAL -> Decimal
  | DUCKDB_TYPE_TIMESTAMP_S -> Timestamp_s
  | DUCKDB_TYPE_TIMESTAMP_MS -> Timestamp_ms
  | DUCKDB_TYPE_TIMESTAMP_NS -> Timestamp_ns
  | DUCKDB_TYPE_ENUM -> Enum
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
    | Decimal : Duckdb_stubs.Decimal.t Ctypes.structure t

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
    | Decimal -> Decimal
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
    | Decimal -> Duckdb_stubs.Decimal.t
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
