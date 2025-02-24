open! Core
open! Duckdb

(* TODO: fixme *)
let%expect_test "maximum value should be normalized correctly" =
  Interval.create ~months:Int32.max_value ~days:Int32.max_value ~micros:Int64.max_value
  |> Interval.normalize
  |> [%sexp_of: Interval.t]
  |> print_s;
  [%expect {| ((months -2075900861) (days 7) (micros 14454775807)) |}]
;;

(* TODO: fixme *)
let%expect_test "interval <-> span roundtrip" =
  Expect_test_helpers_core.require_does_raise ~hide_positions:true [%here] (fun () ->
    Base_quickcheck.Test.run_exn
      (module struct
        type t = Interval.t [@@deriving sexp_of, quickcheck]

        let quickcheck_generator =
          Base_quickcheck.Generator.filter quickcheck_generator ~f:(fun interval ->
            Option.is_some (Interval.to_span interval)
            && Int32.is_non_negative (Interval.months interval))
        ;;
      end)
      ~f:(fun interval ->
        let interval' = Interval.to_span interval |> Option.bind ~f:Interval.of_span in
        [%test_eq: Interval.t option] (Some interval) interval'));
  [%expect
    {|
    ("Base_quickcheck.Test.run: test failed"
      (input (
        (months 0)
        (days   115)
        (micros 21238113138670)))
      (error (
        runtime-lib/runtime.ml.E
        "comparison failed"
        (((
           (months 0)
           (days   115)
           (micros 21238113138670)))
         vs
         ((
           (months 12)
           (days   0)
           (micros 70113138670)))
         (Loc test/interval_test.ml:LINE:COL)))))
    |}]
;;
