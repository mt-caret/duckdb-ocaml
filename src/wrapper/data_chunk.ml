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
