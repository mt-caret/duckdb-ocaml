open! Core
open! Duckdb

let%expect_test "time <-> time_ofday roundtrip" =
  Base_quickcheck.Test.run_exn
    (module struct
      type t = Time_.t [@@deriving sexp_of, quickcheck]

      let quickcheck_generator =
        Base_quickcheck.Generator.filter quickcheck_generator ~f:(fun time ->
          Option.is_some (Time_.to_time_ofday time))
      ;;
    end)
    ~f:(fun time ->
      let time' = Time_.to_time_ofday time |> Option.map ~f:Time_.of_time_ofday in
      [%test_eq: Time_.t option] (Some time) time')
;;
