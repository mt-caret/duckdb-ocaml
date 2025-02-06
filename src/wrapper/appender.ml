open! Core
open! Ctypes

type t = Duckdb_stubs.Appender.t ptr Resource.t

let create conn ?schema table =
  let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
  let t =
    allocate Duckdb_stubs.Appender.t (from_voidp Duckdb_stubs.Appender.t_struct null)
  in
  match Duckdb_stubs.duckdb_appender_create !@conn schema table t with
  | DuckDBSuccess ->
    Resource.create t ~name:"Duckdb.Appender" ~free:(fun t ->
      match Duckdb_stubs.duckdb_appender_destroy t with
      | DuckDBSuccess | DuckDBError ->
        (* DuckDBError here means an error flushing the data, not destroying
             the appender, so it's fine to ignore. If the user cared about this,
             they should've explicitly flushed/closed it + checked the error. *)
        ())
  | DuckDBError -> failwith "Failed to create appender"
;;

let to_result t (duckdb_state : Duckdb_stubs.State.t) ~here =
  let t' = Resource.get_exn t in
  match duckdb_state with
  | DuckDBSuccess -> Ok ()
  | DuckDBError ->
    let error = Duckdb_stubs.duckdb_appender_error !@t' |> Option.value_exn ~here in
    Resource.free t ~here;
    Error error
;;

let close t ~here =
  let t' = Resource.get_exn t in
  Duckdb_stubs.duckdb_appender_close !@t' |> to_result t ~here
;;

let close_exn t ~here = close t ~here |> Result.ok_or_failwith

let flush t ~here =
  let t' = Resource.get_exn t in
  Duckdb_stubs.duckdb_appender_flush !@t' |> to_result t ~here
;;

let flush_exn t ~here = flush t ~here |> Result.ok_or_failwith

let column_count t =
  let t = Resource.get_exn t in
  Duckdb_stubs.duckdb_appender_column_count !@t |> Unsigned.UInt64.to_int
;;

let column_type t idx =
  let t = Resource.get_exn t in
  Duckdb_stubs.duckdb_appender_column_type !@t (Unsigned.UInt64.of_int idx)
  |> Type.Private.with_logical_type ~f:Type.of_logical_type_exn
;;

let column_types t =
  let count = column_count t in
  List.init count ~f:(fun i -> column_type t i)
;;

let append_value (type a) t (type_ : a Type.Typed.t) (value : a) =
  let t = Resource.get_exn t in
  match type_ with
  | Boolean -> Duckdb_stubs.duckdb_append_bool !@t value
  | Tiny_int -> Duckdb_stubs.duckdb_append_int8 !@t value
  | Small_int -> Duckdb_stubs.duckdb_append_int16 !@t value
  | Integer -> Duckdb_stubs.duckdb_append_int32 !@t value
  | Big_int -> Duckdb_stubs.duckdb_append_int64 !@t value
  | U_tiny_int -> Duckdb_stubs.duckdb_append_uint8 !@t value
  | U_small_int -> Duckdb_stubs.duckdb_append_uint16 !@t value
  | U_integer -> Duckdb_stubs.duckdb_append_uint32 !@t value
  | U_big_int -> Duckdb_stubs.duckdb_append_uint64 !@t value
  | Float -> Duckdb_stubs.duckdb_append_float !@t value
  | Double -> Duckdb_stubs.duckdb_append_double !@t value
  | Timestamp -> Duckdb_stubs.duckdb_append_timestamp !@t value
  | Date -> Duckdb_stubs.duckdb_append_date !@t value
  | Time -> Duckdb_stubs.duckdb_append_time !@t value
  | Interval -> Duckdb_stubs.duckdb_append_interval !@t value
  | Huge_int -> Duckdb_stubs.duckdb_append_hugeint !@t value
  | Uhuge_int -> Duckdb_stubs.duckdb_append_uhugeint !@t value
  | Var_char -> Duckdb_stubs.duckdb_append_varchar !@t value
  | Blob ->
    Duckdb_stubs.duckdb_append_varchar_length
      !@t
      (to_voidp (Ctypes_std_views.char_ptr_of_string value))
      (Unsigned.UInt64.of_int (String.length value))
  | Decimal -> failwith "Decimal is not supported for appending"
;;

let rec append_row
  : type a. t -> a Type.Typed.List.t -> a Heterogeneous_list.t -> (unit, string) result
  =
  fun (type a) t (types : a Type.Typed.List.t) (row : a Heterogeneous_list.t) ->
  match types, row with
  | [], [] ->
    let t' = Resource.get_exn t in
    Duckdb_stubs.duckdb_appender_end_row !@t' |> to_result t ~here:[%here]
  | type_ :: types, value :: row ->
    let%bind.Result () = append_value t type_ value |> to_result t ~here:[%here] in
    append_row t types row
;;

let append (type a) t (types : a Type.Typed.List.t) (rows : a Heterogeneous_list.t list) =
  let expected_types = column_types t in
  let argument_types = Type.Typed.List.to_untyped types in
  if not ([%equal: Type.t list] expected_types argument_types)
  then
    raise_s
      [%message
        "Column types do not match"
          (expected_types : Type.t list)
          (argument_types : Type.t list)];
  List.fold_result rows ~init:() ~f:(fun () row -> append_row t types row)
;;

let append_exn t types rows = append t types rows |> Result.ok_or_failwith
