open! Core
open! Duckdb

module Make_quickcheck_module (M : sig
    type t [@@deriving sexp_of, quickcheck]

    val to_time_ns : t -> Time_ns_unix.t option
  end) =
struct
  include M

  let quickcheck_generator =
    Base_quickcheck.Generator.filter quickcheck_generator ~f:(fun timestamp ->
      Option.is_some (to_time_ns timestamp))
  ;;
end

let%expect_test
    "timestamp <-> time_ns roundtrip for timestamps which can be represented as time_ns"
  =
  Base_quickcheck.Test.run_exn
    (module Make_quickcheck_module (Timestamp))
    ~f:(fun timestamp ->
      let timestamp' =
        Timestamp.to_time_ns timestamp |> Option.map ~f:Timestamp.of_time_ns
      in
      [%test_eq: Timestamp.t option] (Some timestamp) timestamp')
;;

let%expect_test
    "timestamp_s <-> time_ns roundtrip for timestamps which can be represented as time_ns"
  =
  Base_quickcheck.Test.run_exn
    (module Make_quickcheck_module (Timestamp.S))
    ~f:(fun timestamp ->
      let timestamp' =
        Timestamp.S.to_time_ns timestamp |> Option.map ~f:Timestamp.S.of_time_ns
      in
      [%test_eq: Timestamp.S.t option] (Some timestamp) timestamp')
;;

let%expect_test
    "timestamp_ms <-> time_ns roundtrip for timestamps which can be represented as \
     time_ns"
  =
  Base_quickcheck.Test.run_exn
    (module Make_quickcheck_module (Timestamp.Ms))
    ~f:(fun timestamp ->
      let timestamp' =
        Timestamp.Ms.to_time_ns timestamp |> Option.map ~f:Timestamp.Ms.of_time_ns
      in
      [%test_eq: Timestamp.Ms.t option] (Some timestamp) timestamp')
;;

let%expect_test
    "timestamp_ns <-> time_ns roundtrip for timestamps which can be represented as \
     time_ns"
  =
  Base_quickcheck.Test.run_exn
    (module Make_quickcheck_module (Timestamp.Ns))
    ~f:(fun timestamp ->
      let timestamp' =
        Timestamp.Ns.to_time_ns timestamp |> Option.map ~f:Timestamp.Ns.of_time_ns
      in
      [%test_eq: Timestamp.Ns.t option] (Some timestamp) timestamp')
;;
