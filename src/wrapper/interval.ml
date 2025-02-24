open! Core
open! Ctypes

type t = Duckdb_stubs.Interval.t structure

let months t = getf t Duckdb_stubs.Interval.months
let days t = getf t Duckdb_stubs.Interval.days
let micros t = getf t Duckdb_stubs.Interval.micros

let sexp_of_t t =
  [%sexp { months : int32 = months t; days : int32 = days t; micros : int64 = micros t }]
;;

let create ~months ~days ~micros =
  let t = make Duckdb_stubs.Interval.t in
  setf t Duckdb_stubs.Interval.months months;
  setf t Duckdb_stubs.Interval.days days;
  setf t Duckdb_stubs.Interval.micros micros;
  t
;;

let normalize t =
  let day_micros = Int64.of_int_exn (24 * 60 * 60 * 1_000_000) in
  let micros = Int64.( % ) (micros t) day_micros in
  let days = Int32.( + ) (days t) (Int64.to_int32_exn (Int64.( / ) micros day_micros)) in
  let month_days = Int32.of_int_exn 30 in
  let months = Int32.( + ) (months t) (Int32.( / ) days month_days) in
  let days = Int32.( % ) days month_days in
  create ~months ~days ~micros
;;

let compare =
  (* We attempt to normalize the interval to a canonical form before comparing. *)
  Comparable.lift [%compare: int32 * int32 * int64] ~f:(fun t ->
    let t = normalize t in
    months t, days t, micros t)
;;

let to_span t =
  let%bind.Option months = months t |> Int32.to_int
  and days = days t |> Int32.to_int
  and micros = micros t |> Int64.to_int in
  let%bind.Option days_span =
    let total_days = (months * 30) + days in
    let max_days_representable =
      Time_ns.Span.div Time_ns.Span.max_value_representable (Time_ns.Span.of_int_day 1)
    in
    match Int63.(max_days_representable < of_int total_days) with
    | true -> None
    | false -> Some (Time_ns.Span.of_int_day total_days)
  and micros_span =
    let max_micros_representable =
      Time_ns.Span.to_int_ns Time_ns.Span.max_value_representable / 1_000
    in
    match max_micros_representable < micros with
    | true -> None
    | false -> Some (Time_ns.Span.of_int_us micros)
  in
  if Time_ns.Span.O.(days_span - micros_span < Time_ns.Span.max_value_representable)
  then Some (Time_ns.Span.( + ) days_span micros_span)
  else None
;;

let of_span span =
  let nanos = Time_ns.Span.to_int_ns span in
  let day_nanos = Time_ns.Span.to_int_ns Time_ns.Span.day in
  let days = nanos / day_nanos in
  let sub_day_nanos = nanos mod day_nanos in
  let months = days / 30 in
  let days = days mod 30 in
  let micros = sub_day_nanos / 1000 in
  match Int32.of_int months, Int32.of_int days with
  | Some months, Some days -> Some (create ~months ~days ~micros:(Int64.of_int micros))
  | _ -> None
;;

let quickcheck_generator =
  Base_quickcheck.Generator.map
    [%quickcheck.generator: int32 * int32 * int64]
    ~f:(fun (months, days, micros) -> create ~months ~days ~micros)
;;

let quickcheck_shrinker =
  Base_quickcheck.Shrinker.map
    [%quickcheck.shrinker: int32 * int32 * int64]
    ~f:(fun (months, days, micros) -> create ~months ~days ~micros)
    ~f_inverse:(fun t ->
      let months = months t in
      let days = days t in
      let micros = micros t in
      months, days, micros)
;;

let quickcheck_observer =
  Base_quickcheck.Observer.unmap
    [%quickcheck.observer: int32 * int32 * int64]
    ~f:(fun t ->
      let months = months t in
      let days = days t in
      let micros = micros t in
      months, days, micros)
;;
