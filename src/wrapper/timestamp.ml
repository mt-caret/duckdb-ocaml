open! Core
open! Ctypes

type t = Duckdb_stubs.Timestamp.t Ctypes.structure

let to_micros_since_epoch (t : t) = getf t Duckdb_stubs.Timestamp.micros
let sexp_of_t t = [%sexp { micros : int64 = to_micros_since_epoch t }]
let compare = Comparable.lift [%compare: int64] ~f:to_micros_since_epoch

let of_micros_since_epoch micros_since_epoch =
  let t = make Duckdb_stubs.Timestamp.t in
  setf t Duckdb_stubs.Timestamp.micros micros_since_epoch;
  t
;;

let num_micros_in_nanos = 1_000

let of_time_ns time_ns =
  of_micros_since_epoch
    (Int64.of_int (Time_ns.to_int_ns_since_epoch time_ns / num_micros_in_nanos))
;;

let to_time_ns t =
  let%bind.Option micros_since_epoch = to_micros_since_epoch t |> Int64.to_int in
  let max_supported_us =
    Time_ns.to_int_ns_since_epoch Time_ns.max_value_representable / num_micros_in_nanos
  in
  let min_supported_us =
    Time_ns.to_int_ns_since_epoch Time_ns.min_value_representable / num_micros_in_nanos
  in
  if micros_since_epoch > max_supported_us || micros_since_epoch < min_supported_us
  then None
  else Some (Time_ns.of_int_ns_since_epoch (micros_since_epoch * num_micros_in_nanos))
;;

let quickcheck_generator =
  Base_quickcheck.Generator.map [%quickcheck.generator: int64] ~f:of_micros_since_epoch
;;

let quickcheck_shrinker =
  Base_quickcheck.Shrinker.map
    [%quickcheck.shrinker: int64]
    ~f:of_micros_since_epoch
    ~f_inverse:to_micros_since_epoch
;;

let quickcheck_observer =
  Base_quickcheck.Observer.unmap [%quickcheck.observer: int64] ~f:to_micros_since_epoch
;;

module S = struct
  type t = Duckdb_stubs.Timestamp_s.t Ctypes.structure

  let to_seconds_since_epoch (t : t) = getf t Duckdb_stubs.Timestamp_s.seconds
  let sexp_of_t t = [%sexp { seconds : int64 = to_seconds_since_epoch t }]
  let compare = Comparable.lift [%compare: int64] ~f:to_seconds_since_epoch

  let of_seconds_since_epoch seconds_since_epoch =
    let t = make Duckdb_stubs.Timestamp_s.t in
    setf t Duckdb_stubs.Timestamp_s.seconds seconds_since_epoch;
    t
  ;;

  let num_seconds_in_nanos = 1_000_000_000

  let of_time_ns time_ns =
    of_seconds_since_epoch
      (Int64.of_int (Time_ns.to_int_ns_since_epoch time_ns / num_seconds_in_nanos))
  ;;

  let to_time_ns t =
    let max_supported_ns =
      Time_ns.max_value_representable |> Time_ns.to_int_ns_since_epoch
    in
    let min_supported_ns =
      Time_ns.min_value_representable |> Time_ns.to_int_ns_since_epoch
    in
    let max_supported_s = max_supported_ns / num_seconds_in_nanos in
    let min_supported_s = min_supported_ns / num_seconds_in_nanos in
    let%bind.Option seconds_since_epoch = to_seconds_since_epoch t |> Int64.to_int in
    if seconds_since_epoch > max_supported_s || seconds_since_epoch < min_supported_s
    then None
    else Some (Time_ns.of_int_ns_since_epoch (seconds_since_epoch * num_seconds_in_nanos))
  ;;

  let quickcheck_generator =
    Base_quickcheck.Generator.map [%quickcheck.generator: int64] ~f:of_seconds_since_epoch
  ;;

  let quickcheck_shrinker =
    Base_quickcheck.Shrinker.map
      [%quickcheck.shrinker: int64]
      ~f:of_seconds_since_epoch
      ~f_inverse:to_seconds_since_epoch
  ;;

  let quickcheck_observer =
    Base_quickcheck.Observer.unmap [%quickcheck.observer: int64] ~f:to_seconds_since_epoch
  ;;
end

module Ms = struct
  type t = Duckdb_stubs.Timestamp_ms.t Ctypes.structure

  let to_millis_since_epoch (t : t) = getf t Duckdb_stubs.Timestamp_ms.millis
  let sexp_of_t t = [%sexp { millis : int64 = to_millis_since_epoch t }]
  let compare = Comparable.lift [%compare: int64] ~f:to_millis_since_epoch

  let of_millis_since_epoch millis_since_epoch =
    let t = make Duckdb_stubs.Timestamp_ms.t in
    setf t Duckdb_stubs.Timestamp_ms.millis millis_since_epoch;
    t
  ;;

  let num_millis_in_nanos = 1_000_000

  let of_time_ns time_ns =
    of_millis_since_epoch
      (Int64.of_int (Time_ns.to_int_ns_since_epoch time_ns / num_millis_in_nanos))
  ;;

  let to_time_ns t =
    let max_supported_ns =
      Time_ns.max_value_representable |> Time_ns.to_int_ns_since_epoch
    in
    let min_supported_ns =
      Time_ns.min_value_representable |> Time_ns.to_int_ns_since_epoch
    in
    let max_supported_ms = max_supported_ns / num_millis_in_nanos in
    let min_supported_ms = min_supported_ns / num_millis_in_nanos in
    let%bind.Option millis_since_epoch = to_millis_since_epoch t |> Int64.to_int in
    if millis_since_epoch > max_supported_ms || millis_since_epoch < min_supported_ms
    then None
    else Some (Time_ns.of_int_ns_since_epoch (millis_since_epoch * num_millis_in_nanos))
  ;;

  let quickcheck_generator =
    Base_quickcheck.Generator.map [%quickcheck.generator: int64] ~f:of_millis_since_epoch
  ;;

  let quickcheck_shrinker =
    Base_quickcheck.Shrinker.map
      [%quickcheck.shrinker: int64]
      ~f:of_millis_since_epoch
      ~f_inverse:to_millis_since_epoch
  ;;

  let quickcheck_observer =
    Base_quickcheck.Observer.unmap [%quickcheck.observer: int64] ~f:to_millis_since_epoch
  ;;
end

module Ns = struct
  type t = Duckdb_stubs.Timestamp_ns.t Ctypes.structure

  let to_nanos_since_epoch (t : t) = getf t Duckdb_stubs.Timestamp_ns.nanos
  let sexp_of_t t = [%sexp { nanos : int64 = to_nanos_since_epoch t }]
  let compare = Comparable.lift [%compare: int64] ~f:to_nanos_since_epoch

  let of_nanos_since_epoch nanos_since_epoch =
    let t = make Duckdb_stubs.Timestamp_ns.t in
    setf t Duckdb_stubs.Timestamp_ns.nanos nanos_since_epoch;
    t
  ;;

  let of_time_ns time_ns =
    of_nanos_since_epoch (Int64.of_int (Time_ns.to_int_ns_since_epoch time_ns))
  ;;

  let to_time_ns t =
    let max_supported_ns =
      Time_ns.max_value_representable |> Time_ns.to_int_ns_since_epoch
    in
    let min_supported_ns =
      Time_ns.min_value_representable |> Time_ns.to_int_ns_since_epoch
    in
    let%bind.Option nanos_since_epoch = to_nanos_since_epoch t |> Int64.to_int in
    if nanos_since_epoch > max_supported_ns || nanos_since_epoch < min_supported_ns
    then None
    else Some (Time_ns.of_int_ns_since_epoch nanos_since_epoch)
  ;;

  let quickcheck_generator =
    Base_quickcheck.Generator.map [%quickcheck.generator: int64] ~f:of_nanos_since_epoch
  ;;

  let quickcheck_shrinker =
    Base_quickcheck.Shrinker.map
      [%quickcheck.shrinker: int64]
      ~f:of_nanos_since_epoch
      ~f_inverse:to_nanos_since_epoch
  ;;

  let quickcheck_observer =
    Base_quickcheck.Observer.unmap [%quickcheck.observer: int64] ~f:to_nanos_since_epoch
  ;;
end
