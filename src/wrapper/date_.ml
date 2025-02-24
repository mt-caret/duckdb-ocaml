open! Core
open! Ctypes

type t = Duckdb_stubs.Date.t Ctypes.structure

let to_days (t : t) = getf t Duckdb_stubs.Date.days
let sexp_of_t t = [%sexp { days : int32 = to_days t }]
let compare = Comparable.lift [%compare: int32] ~f:to_days

let of_days days =
  let t = make Duckdb_stubs.Date.t in
  setf t Duckdb_stubs.Date.days days;
  t
;;

let to_date t =
  let%bind.Option days = to_days t |> Int32.to_int in
  match Date.add_days Date.unix_epoch days with
  | exception Invalid_argument _ ->
    (* Under the hood, [Date.add_days] calls [Date.create_exn], which raises an
       exception when the year is outside of [0..9999]. *)
    None
  | date -> Some date
;;

let of_date date =
  let days = Date.diff date Date.unix_epoch in
  Int32.of_int days |> Option.map ~f:of_days
;;

let quickcheck_generator =
  Base_quickcheck.Generator.map [%quickcheck.generator: int32] ~f:of_days
;;

let quickcheck_shrinker =
  Base_quickcheck.Shrinker.map [%quickcheck.shrinker: int32] ~f:of_days ~f_inverse:to_days
;;

let quickcheck_observer =
  Base_quickcheck.Observer.unmap [%quickcheck.observer: int32] ~f:to_days
;;
