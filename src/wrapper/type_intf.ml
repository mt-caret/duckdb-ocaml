open! Core

module type Typed = sig
  type !'a optional

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
    | Timestamp : Timestamp.t t
    | Date : Date_.t t
    | Time : Time_.t t
    | Interval : Interval.t t
    | Huge_int : Hugeint.t t
    | Uhuge_int : Uhugeint.t t
    | Var_char : string t
    | Blob : string t
    | Timestamp_s : Timestamp.S.t t
    | Timestamp_ms : Timestamp.Ms.t t
    | Timestamp_ns : Timestamp.Ns.t t
    | List : 'a t -> 'a optional list t

  type untyped
  type packed = T : 'a t -> packed

  val to_untyped : 'a t -> untyped
  val of_untyped : untyped -> packed option
  val to_string_hum : 'a t -> 'a -> string

  module List : sig
    type 'a type_ := 'a t

    type _ t =
      | [] : unit t
      | ( :: ) : 'a type_ * 'b t -> ('a * 'b) t

    val to_untyped : 'a t -> untyped list
  end
end

module type Type = sig
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

  val of_logical_type_exn : Duckdb_stubs.Logical_type.t -> t
  val to_logical_type : t -> Duckdb_stubs.Logical_type.t

  module Typed_non_null : Typed with type 'a optional := 'a and type untyped := t
  module Typed : Typed with type 'a optional := 'a option and type untyped := t

  module Private : sig
    val with_logical_type
      :  Duckdb_stubs.Logical_type.t
      -> f:(Duckdb_stubs.Logical_type.t -> 'a)
      -> 'a
  end
end
