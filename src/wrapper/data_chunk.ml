open! Core
open! Ctypes

type t =
  { data_chunk : Duckdb_stubs.Data_chunk.t ptr Resource.t
  ; length : int
  ; schema : (string * Type.t) array
  }
[@@deriving fields ~getters]

let assert_all_valid vector ~len =
  match Duckdb_stubs.duckdb_vector_get_validity vector with
  | None -> ()
  | Some validity ->
    (match len with
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

(* TODO: this is a mess, clean up *)
let get_exn' (type a) data_chunk ~length (type_ : a Type.Typed.t) idx : a array =
  (* TODO: should check that type lines up with schema *)
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector data_chunk (Unsigned.UInt64.of_int idx)
  in
  assert_all_valid vector ~len:length;
  let (T (c_type, read)) = C_type_and_reader.of_type type_ in
  let data = Duckdb_stubs.duckdb_vector_get_data vector |> from_voidp c_type in
  Array.init length ~f:(fun i -> read !@(data +@ i))
;;

let get_exn (type a) t (type_ : a Type.Typed.t) idx : a array =
  get_exn' !@(Resource.get_exn t.data_chunk) ~length:t.length type_ idx
;;

let get_opt (type a) t (type_ : a Type.Typed.t) idx : a option array =
  (* TODO: should check that type lines up with schema *)
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector
      !@(Resource.get_exn t.data_chunk)
      (Unsigned.UInt64.of_int idx)
  in
  let validity = Duckdb_stubs.duckdb_vector_get_validity vector in
  let (T (c_type, read)) = C_type_and_reader.of_type type_ in
  let data = Duckdb_stubs.duckdb_vector_get_data vector |> from_voidp c_type in
  match validity with
  | None -> Array.init t.length ~f:(fun i -> Some (read !@(data +@ i)))
  | Some validity ->
    Array.init t.length ~f:(fun i ->
      match
        Duckdb_stubs.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i)
      with
      | true -> Some (read !@(data +@ i))
      | false -> None)
;;

let free t ~here = Resource.free t.data_chunk ~here

module Private = struct
  let length (data_chunk : Duckdb_stubs.Data_chunk.t ptr) =
    Duckdb_stubs.duckdb_data_chunk_get_size !@data_chunk |> Unsigned.UInt64.to_int
  ;;

  let create data_chunk ~schema =
    let data_chunk = allocate Duckdb_stubs.Data_chunk.t data_chunk in
    { data_chunk =
        Resource.create
          data_chunk
          ~name:"Duckdb.Data_chunk"
          ~free:Duckdb_stubs.duckdb_destroy_data_chunk
    ; length = length data_chunk
    ; schema
    }
  ;;

  let create_do_not_free data_chunk ~schema =
    let data_chunk = allocate Duckdb_stubs.Data_chunk.t data_chunk in
    { data_chunk = Resource.create data_chunk ~name:"Duckdb.Data_chunk" ~free:ignore
    ; length = length data_chunk
    ; schema
    }
  ;;

  let to_ptr t = t.data_chunk

  let get_exn t =
    get_exn'
      t
      ~length:(Duckdb_stubs.duckdb_data_chunk_get_size t |> Unsigned.UInt64.to_int)
  ;;
end
