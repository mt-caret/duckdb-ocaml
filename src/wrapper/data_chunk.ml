open! Core
open! Ctypes

type t =
  { data_chunk : Duckdb_stubs.Data_chunk.t ptr
  ; length : int
  ; schema : (string * Type.t) array
  }
[@@deriving fields ~getters]

let fetch query_result ~f =
  match Duckdb_stubs.duckdb_fetch_chunk (Query.Result.Private.to_struct query_result) with
  | None -> f None
  | Some chunk ->
    let chunk = allocate Duckdb_stubs.Data_chunk.t chunk in
    let length =
      Duckdb_stubs.duckdb_data_chunk_get_size !@chunk |> Unsigned.UInt64.to_int
    in
    protect
      ~f:(fun () ->
        f (Some { data_chunk = chunk; length; schema = Query.Result.schema query_result }))
      ~finally:(fun () -> Duckdb_stubs.duckdb_destroy_data_chunk chunk)
;;

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

(* TODO: this is a mess, clean up *)
let get_exn' (type a) data_chunk ~length (type_ : a Type.Typed.t) idx : a array =
  (* TODO: should check that type lines up with schema *)
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector data_chunk (Unsigned.UInt64.of_int idx)
  in
  assert_all_valid vector ~len:length;
  let data =
    Duckdb_stubs.duckdb_vector_get_data vector |> from_voidp (Type.Typed.to_c_type type_)
  in
  Array.init length ~f:(fun i -> !@(data +@ i))
;;

let get_exn (type a) t (type_ : a Type.Typed.t) idx : a array =
  get_exn' !@(t.data_chunk) ~length:t.length type_ idx
;;

let get_opt (type a) t (type_ : a Type.Typed.t) idx : a option array =
  (* TODO: should check that type lines up with schema *)
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector
      !@(t.data_chunk)
      (Unsigned.UInt64.of_int idx)
  in
  let validity = Duckdb_stubs.duckdb_vector_get_validity vector in
  let data =
    Duckdb_stubs.duckdb_vector_get_data vector |> from_voidp (Type.Typed.to_c_type type_)
  in
  match validity with
  | None -> Array.init t.length ~f:(fun i -> Some !@(data +@ i))
  | Some validity ->
    Array.init t.length ~f:(fun i ->
      match
        Duckdb_stubs.duckdb_validity_row_is_valid validity (Unsigned.UInt64.of_int i)
      with
      | true -> Some !@(data +@ i)
      | false -> None)
;;

module Private = struct
  let to_ptr t = t.data_chunk

  let get_exn t =
    get_exn'
      t
      ~length:(Duckdb_stubs.duckdb_data_chunk_get_size t |> Unsigned.UInt64.to_int)
  ;;
end
