open! Core

type t = Duckdb_stubs.Timestamp.t Ctypes.structure
[@@deriving compare, sexp_of, quickcheck]

val to_micros_since_epoch : t -> int64
val of_micros_since_epoch : int64 -> t
val of_time_ns : Time_ns.t -> t
val to_time_ns : t -> Time_ns.t option

module S : sig
  type t = Duckdb_stubs.Timestamp_s.t Ctypes.structure
  [@@deriving compare, sexp_of, quickcheck]

  val to_seconds_since_epoch : t -> int64
  val of_seconds_since_epoch : int64 -> t
  val of_time_ns : Time_ns.t -> t
  val to_time_ns : t -> Time_ns.t option
end

module Ms : sig
  type t = Duckdb_stubs.Timestamp_ms.t Ctypes.structure
  [@@deriving compare, sexp_of, quickcheck]

  val to_millis_since_epoch : t -> int64
  val of_millis_since_epoch : int64 -> t
  val of_time_ns : Time_ns.t -> t
  val to_time_ns : t -> Time_ns.t option
end

module Ns : sig
  type t = Duckdb_stubs.Timestamp_ns.t Ctypes.structure
  [@@deriving compare, sexp_of, quickcheck]

  val to_nanos_since_epoch : t -> int64
  val of_nanos_since_epoch : int64 -> t
  val of_time_ns : Time_ns.t -> t
  val to_time_ns : t -> Time_ns.t option
end
