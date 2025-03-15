open! Core
open! Ctypes

type t =
  { data_chunk : Duckdb_stubs.Data_chunk.t Resource.t
  ; length : int
  }

let length t = t.length

let get_exn t type_ idx =
  let data_chunk = Resource.get_exn t.data_chunk in
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector !@data_chunk (Unsigned.UInt64.of_int idx)
  in
  Vector.Private.get_exn vector type_ t.length
;;

let get_opt t type_ idx =
  let data_chunk = Resource.get_exn t.data_chunk in
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector !@data_chunk (Unsigned.UInt64.of_int idx)
  in
  Vector.Private.get_opt vector type_ t.length
;;

let free t ~here = Resource.free t.data_chunk ~here

let to_string_hum ?(bars = `Unicode) ~column_count t =
  (* Simplified implementation that doesn't rely on Vector.get_type *)
  match t.length, column_count with
  | 0, _ | _, 0 -> ""
  | _, _ ->
    (* Convert `None to `Ascii since to_string_noattr doesn't accept `None *)
    let bars' =
      match bars with
      | `None -> `Ascii
      | (`Ascii | `Unicode) as b -> b
    in
    let columns =
      List.init column_count ~f:(fun idx ->
        let name = sprintf "Column %d" idx in
        Ascii_table_kernel.Column.create name (fun i ->
          if i < t.length then "..." else ""))
    in
    List.range 0 t.length |> Ascii_table_kernel.to_string_noattr columns ~bars:bars'
;;

module Private = struct
  let length (data_chunk : Duckdb_stubs.Data_chunk.t ptr) =
    Duckdb_stubs.duckdb_data_chunk_get_size !@data_chunk |> Unsigned.UInt64.to_int
  ;;

  let create data_chunk =
    let t =
      { data_chunk = Resource.create data_chunk ~f:Duckdb_stubs.duckdb_data_chunk_destroy
      ; length = length data_chunk
      }
    in
    t
  ;;

  let create_do_not_free data_chunk =
    let t =
      { data_chunk = Resource.create_do_not_free data_chunk; length = length data_chunk }
    in
    t
  ;;
end
