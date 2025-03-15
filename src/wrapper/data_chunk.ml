open! Core
open! Ctypes

type t =
  { data_chunk : Duckdb_stubs.Data_chunk.t ptr Resource.t
  ; length : int
  }
[@@deriving fields ~getters]

let get_exn t type_ idx =
  (* TODO: should check that type lines up with schema *)
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector
      !@(Resource.get_exn t.data_chunk)
      (Unsigned.UInt64.of_int idx)
  in
  Vector.to_array_exn vector type_ ~length:t.length
;;

let get_opt t type_ idx =
  (* TODO: should check that type lines up with schema *)
  let vector =
    Duckdb_stubs.duckdb_data_chunk_get_vector
      !@(Resource.get_exn t.data_chunk)
      (Unsigned.UInt64.of_int idx)
  in
  Vector.to_option_array vector type_ ~length:t.length
;;

let free t ~here = Resource.free t.data_chunk ~here

let column_count t =
  (* Count vectors by trying to access them until we get an exception *)
  let rec count_vectors idx =
    try
      let _ =
        Duckdb_stubs.duckdb_data_chunk_get_vector
          !@(Resource.get_exn t.data_chunk)
          (Unsigned.UInt64.of_int idx)
      in
      count_vectors (idx + 1)
    with
    | _ -> idx
  in
  count_vectors 0
;;

let to_string_hum ?(bars = `Unicode) t =
  (* Simplified implementation that doesn't rely on Vector.get_type *)
  (* If there are no rows, return an empty string *)
  if t.length = 0
  then ""
  else (
    let col_count = column_count t in
    if col_count = 0
    then ""
    else (
      (* Convert `None to `Ascii since to_string_noattr doesn't accept `None *)
      let bars' =
        match bars with
        | `None -> `Ascii
        | (`Ascii | `Unicode) as b -> b
      in
      let columns =
        List.init col_count ~f:(fun idx ->
          let name = sprintf "Column %d" idx in
          Ascii_table_kernel.Column.create name (fun i ->
            if i < t.length then "..." else ""))
      in
      List.range 0 t.length |> Ascii_table_kernel.to_string_noattr columns ~bars:bars'))
;;

module Private = struct
  let length (data_chunk : Duckdb_stubs.Data_chunk.t ptr) =
    Duckdb_stubs.duckdb_data_chunk_get_size !@data_chunk |> Unsigned.UInt64.to_int
  ;;

  let create data_chunk =
    let data_chunk = allocate Duckdb_stubs.Data_chunk.t data_chunk in
    { data_chunk =
        Resource.create
          data_chunk
          ~name:"Duckdb.Data_chunk"
          ~free:Duckdb_stubs.duckdb_destroy_data_chunk
    ; length = length data_chunk
    }
  ;;

  let create_do_not_free data_chunk =
    let data_chunk = allocate Duckdb_stubs.Data_chunk.t data_chunk in
    { data_chunk = Resource.create data_chunk ~name:"Duckdb.Data_chunk" ~free:ignore
    ; length = length data_chunk
    }
  ;;

  let to_ptr t = t.data_chunk
end
