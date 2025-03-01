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

module How_to_read_non_null = struct
  type 'a t =
    | Direct : 'a Ctypes.typ -> 'a t
    | String : string t
    | List : 'a Type.Typed_non_null.t -> 'a list t

  let of_type (type a) (type_ : a Type.Typed_non_null.t) : a t =
    match type_ with
    | Boolean -> Direct bool
    | Tiny_int -> Direct int8_t
    | Small_int -> Direct int16_t
    | Integer -> Direct int32_t
    | Big_int -> Direct int64_t
    | U_tiny_int -> Direct uint8_t
    | U_small_int -> Direct uint16_t
    | U_integer -> Direct uint32_t
    | U_big_int -> Direct uint64_t
    | Float -> Direct float
    | Double -> Direct double
    | Timestamp -> Direct Duckdb_stubs.Timestamp.t
    | Date -> Direct Duckdb_stubs.Date.t
    | Time -> Direct Duckdb_stubs.Time.t
    | Interval -> Direct Duckdb_stubs.Interval.t
    | Huge_int -> Direct Duckdb_stubs.Hugeint.t
    | Uhuge_int -> Direct Duckdb_stubs.Uhugeint.t
    | Var_char -> String
    | Blob -> String
    | Timestamp_s -> Direct Duckdb_stubs.Timestamp_s.t
    | Timestamp_ms -> Direct Duckdb_stubs.Timestamp_ms.t
    | Timestamp_ns -> Direct Duckdb_stubs.Timestamp_ns.t
    | List child -> List child
  ;;
end

(* TODO: this duplication is pretty sad. *)
module How_to_read = struct
  type 'a t =
    | Direct : 'a Ctypes.typ -> 'a t
    | String : string t
    | List : 'a Type.Typed.t -> 'a option list t

  let of_type (type a) (type_ : a Type.Typed.t) : a t =
    match type_ with
    | Boolean -> Direct bool
    | Tiny_int -> Direct int8_t
    | Small_int -> Direct int16_t
    | Integer -> Direct int32_t
    | Big_int -> Direct int64_t
    | U_tiny_int -> Direct uint8_t
    | U_small_int -> Direct uint16_t
    | U_integer -> Direct uint32_t
    | U_big_int -> Direct uint64_t
    | Float -> Direct float
    | Double -> Direct double
    | Timestamp -> Direct Duckdb_stubs.Timestamp.t
    | Date -> Direct Duckdb_stubs.Date.t
    | Time -> Direct Duckdb_stubs.Time.t
    | Interval -> Direct Duckdb_stubs.Interval.t
    | Huge_int -> Direct Duckdb_stubs.Hugeint.t
    | Uhuge_int -> Direct Duckdb_stubs.Uhugeint.t
    | Var_char -> String
    | Blob -> String
    | Timestamp_s -> Direct Duckdb_stubs.Timestamp_s.t
    | Timestamp_ms -> Direct Duckdb_stubs.Timestamp_ms.t
    | Timestamp_ns -> Direct Duckdb_stubs.Timestamp_ns.t
    | List child -> List child
  ;;
end

let duckdb_string_to_string s =
  Ctypes.string_from_ptr
    (Duckdb_stubs.duckdb_string_t_data (addr s))
    ~length:(Duckdb_stubs.duckdb_string_t_length s |> Unsigned.UInt32.to_int)
;;

let rec to_array_exn : type a. t -> a Type.Typed_non_null.t -> length:int -> a array =
  fun t type_ ~length ->
  assert_all_valid t ~length;
  match How_to_read_non_null.of_type type_ with
  | Direct c_type ->
    let data = Duckdb_stubs.duckdb_vector_get_data t |> from_voidp c_type in
    Array.init length ~f:(fun i -> !@(data +@ i))
  | String ->
    let data =
      Duckdb_stubs.duckdb_vector_get_data t |> from_voidp Duckdb_stubs.String.t
    in
    Array.init length ~f:(fun i -> duckdb_string_to_string !@(data +@ i))
  | List child ->
    let data =
      Duckdb_stubs.duckdb_vector_get_data t |> from_voidp Duckdb_stubs.List_entry.t
    in
    let flat_array =
      to_array_exn
        (Duckdb_stubs.duckdb_list_vector_get_child t)
        child
        ~length:(Duckdb_stubs.duckdb_list_vector_get_size t |> Unsigned.UInt64.to_int)
    in
    Array.init length ~f:(fun i ->
      let entry = data +@ i in
      let offset =
        getf !@entry Duckdb_stubs.List_entry.offset |> Unsigned.UInt64.to_int
      in
      let length =
        getf !@entry Duckdb_stubs.List_entry.length |> Unsigned.UInt64.to_int
      in
      Array.sub flat_array ~pos:offset ~len:length |> Array.to_list)
;;

let rec to_option_array : type a. t -> a Type.Typed.t -> length:int -> a option array =
  fun (type a) t (type_ : a Type.Typed.t) ~length ->
  let is_valid =
    match Duckdb_stubs.duckdb_vector_get_validity t with
    | None -> fun _ -> true
    | Some validity ->
      fun i ->
        Duckdb_stubs.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i)
  in
  match How_to_read.of_type type_ with
  | Direct c_type ->
    let data = Duckdb_stubs.duckdb_vector_get_data t |> from_voidp c_type in
    Array.init length ~f:(fun i ->
      match is_valid i with
      | true -> Some !@(data +@ i)
      | false -> None)
  | String ->
    let data =
      Duckdb_stubs.duckdb_vector_get_data t |> from_voidp Duckdb_stubs.String.t
    in
    Array.init length ~f:(fun i ->
      match is_valid i with
      | true -> Some (duckdb_string_to_string !@(data +@ i))
      | false -> None)
  | List child ->
    let data =
      Duckdb_stubs.duckdb_vector_get_data t |> from_voidp Duckdb_stubs.List_entry.t
    in
    let flat_array =
      to_option_array
        (Duckdb_stubs.duckdb_list_vector_get_child t)
        child
        ~length:(Duckdb_stubs.duckdb_list_vector_get_size t |> Unsigned.UInt64.to_int)
    in
    Array.init length ~f:(fun i ->
      match is_valid i with
      | true ->
        let entry = data +@ i in
        let offset =
          getf !@entry Duckdb_stubs.List_entry.offset |> Unsigned.UInt64.to_int
        in
        let length =
          getf !@entry Duckdb_stubs.List_entry.length |> Unsigned.UInt64.to_int
        in
        Some (Array.sub flat_array ~pos:offset ~len:length |> Array.to_list)
      | false -> None)
;;

module How_to_write = struct
  type 'a t =
    | Direct : 'a Ctypes.typ -> 'a t
    | String : string t
end

let set_array (type a) t (type_ : a Type.Typed_non_null.t) (values : a array) =
  let (how_to_write : a How_to_write.t) =
    match type_ with
    | Boolean -> Direct bool
    | Tiny_int -> Direct int8_t
    | Small_int -> Direct int16_t
    | Integer -> Direct int32_t
    | Big_int -> Direct int64_t
    | U_tiny_int -> Direct uint8_t
    | U_small_int -> Direct uint16_t
    | U_integer -> Direct uint32_t
    | U_big_int -> Direct uint64_t
    | Float -> Direct float
    | Double -> Direct double
    | Timestamp -> Direct Duckdb_stubs.Timestamp.t
    | Date -> Direct Duckdb_stubs.Date.t
    | Time -> Direct Duckdb_stubs.Time.t
    | Interval -> Direct Duckdb_stubs.Interval.t
    | Huge_int -> Direct Duckdb_stubs.Hugeint.t
    | Uhuge_int -> Direct Duckdb_stubs.Uhugeint.t
    | Var_char -> String
    | Blob -> String
    | Timestamp_s -> Direct Duckdb_stubs.Timestamp_s.t
    | Timestamp_ms -> Direct Duckdb_stubs.Timestamp_ms.t
    | Timestamp_ns -> Direct Duckdb_stubs.Timestamp_ns.t
    | List _child -> failwith "Unimplemented"
  in
  match how_to_write with
  | Direct c_type ->
    let data = Duckdb_stubs.duckdb_vector_get_data t |> from_voidp c_type in
    Array.iteri values ~f:(fun i value -> data +@ i <-@ value)
  | String ->
    Array.iteri values ~f:(fun i value ->
      Duckdb_stubs.duckdb_vector_assign_string_element_len
        t
        (Unsigned.UInt64.of_int i)
        value
        (Unsigned.UInt64.of_int (String.length value)))
;;
