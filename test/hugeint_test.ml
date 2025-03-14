open! Core
open! Ctypes

(* Tests for Hugeint and Uhugeint modules *)

let%expect_test "hugeint_basic" =
  (* Test hugeint creation and conversion *)
  let hugeint =
    Duckdb.Hugeint.create ~lower:(Unsigned.UInt64.of_int 1234567890) ~upper:0L
  in
  let float_val = Duckdb.Hugeint.to_float hugeint in
  [%message "Hugeint to float" ~value:(float_val : float)] |> print_s;
  [%expect {| ("Hugeint to float" (value 1234567890)) |}];
  (* Test float to hugeint conversion *)
  let hugeint2 = Duckdb.Hugeint.of_float 9876543210.0 in
  let lower = Duckdb.Hugeint.lower hugeint2 in
  let upper = Duckdb.Hugeint.upper hugeint2 in
  [%message
    "Float to hugeint"
      ~lower:(Unsigned.UInt64.to_string lower)
      ~upper:(Unsigned.UInt64.to_string upper)]
  |> print_s;
  [%expect {| ("Float to hugeint" (lower 9876543210) (upper 0)) |}]
;;

let%expect_test "hugeint_in_database" =
  Duckdb.Database.with_path ":memory:" ~f:(fun db ->
    Duckdb.Connection.with_connection db ~f:(fun conn ->
      (* Create a table with hugeint column *)
      Duckdb.Query.run_exn' conn {|CREATE TABLE hugeint_test(a HUGEINT)|};
      Duckdb.Query.run_exn' conn {|INSERT INTO hugeint_test VALUES (1234567890123456789)|};
      (* Query the hugeint value *)
      Duckdb.Query.run_exn conn "SELECT * FROM hugeint_test" ~f:(fun res ->
        let result = Duckdb.Result_.to_string_hum res ~bars:`Unicode in
        [%message "Hugeint in database" ~result:(result : string)] |> print_s);
      [%expect
        {|
        ("Hugeint in database"
         (result
          "┌────────────────────┐\n│ a                  │\n│ Huge_int           │\n├────────────────────┤\n│ 1234567890123456789│\n└────────────────────┘"))
        |}]))
;;

let%expect_test "uhugeint_basic" =
  (* Test uhugeint creation and conversion *)
  let uhugeint =
    Duckdb.Uhugeint.create ~lower:(Unsigned.UInt64.of_int 1234567890) ~upper:(Unsigned.UInt64.of_int 0)
  in
  let float_val = Duckdb.Uhugeint.to_float uhugeint in
  [%message "Uhugeint to float" ~value:(float_val : float)] |> print_s;
  [%expect {| ("Uhugeint to float" (value 1234567890)) |}];
  (* Test float to uhugeint conversion *)
  let uhugeint2 = Duckdb.Uhugeint.of_float 9876543210.0 in
  let lower = Duckdb.Uhugeint.lower uhugeint2 in
  let upper = Duckdb.Uhugeint.upper uhugeint2 in
  [%message
    "Float to uhugeint"
      ~lower:(Unsigned.UInt64.to_string lower)
      ~upper:(Unsigned.UInt64.to_string upper)]
  |> print_s;
  [%expect {| ("Float to uhugeint" (lower 9876543210) (upper 0)) |}]
;;
