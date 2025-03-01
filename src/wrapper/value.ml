open! Core
open! Ctypes

type t = Duckdb_stubs.Value.t

let rec create : type a. a Type.Typed.t -> a -> t =
  fun type_ value ->
  match type_ with
  | Boolean -> Duckdb_stubs.duckdb_create_bool value
  | Tiny_int -> Duckdb_stubs.duckdb_create_int8 value
  | Small_int -> Duckdb_stubs.duckdb_create_int16 value
  | Integer -> Duckdb_stubs.duckdb_create_int32 value
  | Big_int -> Duckdb_stubs.duckdb_create_int64 value
  | U_tiny_int -> Duckdb_stubs.duckdb_create_uint8 value
  | U_small_int -> Duckdb_stubs.duckdb_create_uint16 value
  | U_integer -> Duckdb_stubs.duckdb_create_uint32 value
  | U_big_int -> Duckdb_stubs.duckdb_create_uint64 value
  | Float -> Duckdb_stubs.duckdb_create_float value
  | Double -> Duckdb_stubs.duckdb_create_double value
  | Timestamp -> Duckdb_stubs.duckdb_create_timestamp value
  | Date -> Duckdb_stubs.duckdb_create_date value
  | Time -> Duckdb_stubs.duckdb_create_time value
  | Interval -> Duckdb_stubs.duckdb_create_interval value
  | Huge_int -> Duckdb_stubs.duckdb_create_hugeint value
  | Uhuge_int -> Duckdb_stubs.duckdb_create_uhugeint value
  | Var_char -> Duckdb_stubs.duckdb_create_varchar value
  | Blob ->
    Duckdb_stubs.duckdb_create_blob
      (from_voidp uint8_t (to_voidp (Ctypes_std_views.char_ptr_of_string value)))
      (Unsigned.UInt64.of_int (String.length value))
  | Timestamp_s -> Duckdb_stubs.duckdb_create_timestamp_s value
  | Timestamp_ms -> Duckdb_stubs.duckdb_create_timestamp_ms value
  | Timestamp_ns -> Duckdb_stubs.duckdb_create_timestamp_ns value
  | List child ->
    let count = List.length value in
    let value_ptr = allocate_n Duckdb_stubs.Value.t ~count in
    List.iteri value ~f:(fun i value -> value_ptr +@ i <-@ create child value);
    Type.Typed.to_untyped child
    |> Type.to_logical_type
    |> Type.Private.with_logical_type ~f:(fun logical_type ->
      let t =
        Duckdb_stubs.duckdb_create_list_value
          logical_type
          (Some value_ptr)
          (Unsigned.UInt64.of_int count)
      in
      List.range 0 count
      |> List.iter ~f:(fun i -> Duckdb_stubs.duckdb_destroy_value (value_ptr +@ i));
      t)
;;
