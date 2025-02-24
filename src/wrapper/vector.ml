open! Core
open! Ctypes

type t = Duckdb_stubs.Vector.t

let assert_all_valid vector ~length =
  match Duckdb_stubs.duckdb_vector_get_validity vector with
  | None -> ()
  | Some validity ->
    (match length with
     | 0 -> ()
     | len ->
       let all_bits_set =
         Unsigned.UInt64.sub
           (Unsigned.UInt64.shift_left Unsigned.UInt64.one len)
           Unsigned.UInt64.one
       in
       if not Unsigned.UInt64.(equal (logand all_bits_set !@validity) all_bits_set)
       then
         raise_s
           [%message
             "Not all rows are valid" ~validity:(Unsigned.UInt64.to_int !@validity : int)])
;;

module C_type_and_reader = struct
  type 'a t = T : 'a Ctypes.typ * ('a -> 'b) -> 'b t

  let of_type (type a) (type_ : a Type.Typed.t) : a t =
    match type_ with
    | Boolean -> T (bool, Fn.id)
    | Tiny_int -> T (int8_t, Fn.id)
    | Small_int -> T (int16_t, Fn.id)
    | Integer -> T (int32_t, Fn.id)
    | Big_int -> T (int64_t, Fn.id)
    | U_tiny_int -> T (uint8_t, Fn.id)
    | U_small_int -> T (uint16_t, Fn.id)
    | U_integer -> T (uint32_t, Fn.id)
    | U_big_int -> T (uint64_t, Fn.id)
    | Float -> T (float, Fn.id)
    | Double -> T (double, Fn.id)
    | Timestamp -> T (Duckdb_stubs.Timestamp.t, Fn.id)
    | Date -> T (Duckdb_stubs.Date.t, Fn.id)
    | Time -> T (Duckdb_stubs.Time.t, Fn.id)
    | Interval -> T (Duckdb_stubs.Interval.t, Fn.id)
    | Huge_int -> T (Duckdb_stubs.Hugeint.t, Fn.id)
    | Uhuge_int -> T (Duckdb_stubs.Uhugeint.t, Fn.id)
    | Var_char ->
      T (Duckdb_stubs.String.t, fun s -> Duckdb_stubs.duckdb_string_t_data (addr s))
    | Blob ->
      T (Duckdb_stubs.String.t, fun s -> Duckdb_stubs.duckdb_string_t_data (addr s))
    | Timestamp_s -> T (Duckdb_stubs.Timestamp_s.t, Fn.id)
    | Timestamp_ms -> T (Duckdb_stubs.Timestamp_ms.t, Fn.id)
    | Timestamp_ns -> T (Duckdb_stubs.Timestamp_ns.t, Fn.id)
  ;;
end

let to_array_exn (type a) t (type_ : a Type.Typed.t) ~length : a array =
  assert_all_valid t ~length;
  let (T (c_type, read)) = C_type_and_reader.of_type type_ in
  let data = Duckdb_stubs.duckdb_vector_get_data t |> from_voidp c_type in
  Array.init length ~f:(fun i -> read !@(data +@ i))
;;

let to_option_array (type a) t (type_ : a Type.Typed.t) ~length : a option array =
  let validity = Duckdb_stubs.duckdb_vector_get_validity t in
  let (T (c_type, read)) = C_type_and_reader.of_type type_ in
  let data = Duckdb_stubs.duckdb_vector_get_data t |> from_voidp c_type in
  match validity with
  | None -> Array.init length ~f:(fun i -> Some (read !@(data +@ i)))
  | Some validity ->
    Array.init length ~f:(fun i ->
      match
        Duckdb_stubs.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i)
      with
      | true -> Some (read !@(data +@ i))
      | false -> None)
;;

let set_array (type a) t (type_ : a Type.Typed.t) (values : a array) =
  let (c_type : a Ctypes.typ) =
    match type_ with
    | Boolean -> bool
    | Tiny_int -> int8_t
    | Small_int -> int16_t
    | Integer -> int32_t
    | Big_int -> int64_t
    | U_tiny_int -> uint8_t
    | U_small_int -> uint16_t
    | U_integer -> uint32_t
    | U_big_int -> uint64_t
    | Float -> float
    | Double -> double
    | Timestamp -> Duckdb_stubs.Timestamp.t
    | Date -> Duckdb_stubs.Date.t
    | Time -> Duckdb_stubs.Time.t
    | Interval -> Duckdb_stubs.Interval.t
    | Huge_int -> Duckdb_stubs.Hugeint.t
    | Uhuge_int -> Duckdb_stubs.Uhugeint.t
    | Var_char -> failwith "Var_char is not writable"
    | Blob -> failwith "Blob is not writable"
    | Timestamp_s -> Duckdb_stubs.Timestamp_s.t
    | Timestamp_ms -> Duckdb_stubs.Timestamp_ms.t
    | Timestamp_ns -> Duckdb_stubs.Timestamp_ns.t
  in
  let data = Duckdb_stubs.duckdb_vector_get_data t |> from_voidp c_type in
  Array.iteri values ~f:(fun i value -> data +@ i <-@ value)
;;
