open! Core
open! Ctypes

type t = Duckdb_stubs.Result.t structure Resource.t

let create () =
  let result = make Duckdb_stubs.Result.t in
  Resource.create result ~name:"Duckdb.Resource" ~free:(fun result ->
    Duckdb_stubs.duckdb_destroy_result (addr result))
;;

let column_count (t : t) =
  Resource.get_exn t |> addr |> Duckdb_stubs.duckdb_column_count |> Unsigned.UInt64.to_int
;;

let schema (t : t) =
  let t' = Resource.get_exn t in
  column_count t
  |> Array.init ~f:(fun i ->
    let i = Unsigned.UInt64.of_int i in
    let name = Duckdb_stubs.duckdb_column_name (addr t') i in
    let type_ =
      Duckdb_stubs.duckdb_column_logical_type (addr t') i
      |> Type.Private.with_logical_type ~f:Type.of_logical_type_exn
    in
    name, type_)
;;

let fetch (t : t) ~f =
  let t' = Resource.get_exn t in
  match Duckdb_stubs.duckdb_fetch_chunk t' with
  | None -> f None
  | Some data_chunk ->
    Data_chunk.Private.create data_chunk
    |> protectx
         ~f:(fun data_chunk -> f (Some data_chunk))
         ~finally:(fun data_chunk ->
           Data_chunk.Private.to_ptr data_chunk |> Resource.free ~here:[%here])
;;

let fetch_all (t : t) =
  let open struct
    type t =
      | T :
          { name : string
          ; type_ : 'a Type.Typed.t
          ; mutable results : 'a option array list
          }
          -> t
  end in
  let results =
    schema t
    |> Array.map ~f:(fun (name, type_) ->
      match Type.Typed.of_untyped type_ with
      | None -> raise_s [%message "Unsupported type" (type_ : Type.t)]
      | Some (T type_) -> T { name; type_; results = [] })
  in
  let total_length = ref 0 in
  while
    match Duckdb_stubs.duckdb_fetch_chunk (Resource.get_exn t) with
    | None -> false
    | Some data_chunk ->
      let data_chunk = Data_chunk.Private.create data_chunk in
      total_length := !total_length + Data_chunk.length data_chunk;
      Array.iteri results ~f:(fun i (T result) ->
        result.results <- Data_chunk.get_opt data_chunk result.type_ i :: result.results);
      Data_chunk.Private.to_ptr data_chunk |> Resource.free ~here:[%here];
      true
  do
    ()
  done;
  ( !total_length
  , Array.map results ~f:(fun (T { name; type_; results }) ->
      name, Packed_column.T (type_, Array.concat (List.rev results))) )
;;

let to_string_hum ?(bars = `Unicode) t =
  let n, fetch_result = fetch_all t in
  let columns =
    Array.to_list fetch_result
    |> List.map ~f:(fun (name, packed_array) ->
      let type_name =
        (match packed_array with
         | T_non_null (type_, _) -> Type.Typed_non_null.to_untyped type_
         | T (type_, _) -> Type.Typed.to_untyped type_)
        |> [%sexp_of: Type.t]
        |> Sexp.to_string
      in
      Ascii_table_kernel.Column.create [%string "%{name}\n%{type_name}"] (fun i ->
        match packed_array with
        | T_non_null (type_, array) ->
          Array.get array i |> Type.Typed_non_null.to_string_hum type_
        | T (type_, array) ->
          (match Array.get array i with
           | None -> "null"
           | Some value -> Type.Typed.to_string_hum type_ value)))
  in
  List.range 0 n |> Ascii_table_kernel.to_string_noattr columns ~bars
;;

module Private = struct
  let to_struct = Fn.id
end
