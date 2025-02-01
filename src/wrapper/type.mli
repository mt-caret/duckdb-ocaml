open! Core

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

val of_logical_type_exn : Duckdb_stubs.duckdb_logical_type -> t

module Typed : sig
  type untyped := t

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

  val to_untyped : 'a t -> untyped
  val to_c_type : 'a t -> 'a Ctypes_static.typ

  module List : sig
    type 'a type_ := 'a t

    type _ t =
      | [] : Core.never_returns t
      | ( :: ) : 'a type_ * 'b t -> ('a * 'b) t

    val to_untyped : 'a t -> untyped list
  end
end

module Private : sig
  val with_logical_type
    :  Duckdb_stubs.duckdb_logical_type
    -> f:(Duckdb_stubs.duckdb_logical_type -> 'a)
    -> 'a
end
