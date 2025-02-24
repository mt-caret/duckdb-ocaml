open! Core
open! Ctypes

type t = Duckdb_stubs.Time.t structure

let to_micros_since_start_of_day (t : t) = getf t Duckdb_stubs.Time.micros
let sexp_of_t t = [%sexp { micros : int64 = to_micros_since_start_of_day t }]
let compare = Comparable.lift [%compare: int64] ~f:to_micros_since_start_of_day

let of_micros_since_start_of_day micros_since_start_of_day =
  let t = make Duckdb_stubs.Time.t in
  setf t Duckdb_stubs.Time.micros micros_since_start_of_day;
  t
;;

let to_time_ofday t =
  let%bind.Option micros_since_start_of_day =
    to_micros_since_start_of_day t |> Int64.to_int
  in
  let span_since_start_of_day = Time_ns.Span.of_int_us micros_since_start_of_day in
  match
    (* [Time_ns.Ofday.of_span_since_start_of_day_exn] only accepts values in
       this region. *)
    Time_ns.Span.between
      span_since_start_of_day
      ~low:Time_ns.Span.zero
      ~high:(Time_ns.Span.of_int_day 1)
  with
  | true -> Some (Time_ns.Ofday.of_span_since_start_of_day_exn span_since_start_of_day)
  | false -> None
;;

let of_time_ofday time_ofday =
  let micros_since_start_of_day =
    Time_ns.Ofday.to_span_since_start_of_day time_ofday |> Time_ns.Span.to_int_us
  in
  of_micros_since_start_of_day (Int64.of_int micros_since_start_of_day)
;;

let quickcheck_generator =
  Base_quickcheck.Generator.map
    [%quickcheck.generator: int64]
    ~f:of_micros_since_start_of_day
;;

let quickcheck_shrinker =
  Base_quickcheck.Shrinker.map
    [%quickcheck.shrinker: int64]
    ~f:of_micros_since_start_of_day
    ~f_inverse:to_micros_since_start_of_day
;;

let quickcheck_observer =
  Base_quickcheck.Observer.unmap
    [%quickcheck.observer: int64]
    ~f:to_micros_since_start_of_day
;;
