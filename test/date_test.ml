open! Core
open! Duckdb

let%expect_test "date <-> core.date roundtrip" =
  Base_quickcheck.Test.run_exn
    (module struct
      type t = Date_.t [@@deriving sexp_of, quickcheck]

      let quickcheck_generator =
        Base_quickcheck.Generator.filter quickcheck_generator ~f:(fun date ->
          Option.is_some (Date_.to_date date))
      ;;
    end)
    ~f:(fun date ->
      let date' = Date_.to_date date |> Option.bind ~f:Date_.of_date in
      [%test_eq: Date_.t option] (Some date) date')
;;
