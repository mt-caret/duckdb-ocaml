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

let to_string_hum ?(bars = `Unicode) ~column_count t =
  match t.length, column_count with
  | 0, _ -> "" (* Empty data chunk *)
  | _, 0 -> "" (* No columns to display *)
  | _, _ ->
    let columns =
      List.init column_count ~f:(fun idx ->
        let name = sprintf "Column %d" idx in
        Ascii_table_kernel.Column.create name (fun i ->
          if i < t.length then "..." else ""))
    in
    List.range 0 t.length |> Ascii_table_kernel.to_string_noattr columns ~bars
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

  (* Expose the data_chunk resource for use in Result_.ml *)
  let to_ptr t = t.data_chunk
end
