open! Core
open! Ctypes

module Error = struct
  type t =
    { kind : Duckdb_stubs.Error_type.t
    ; message : string
    }
  [@@deriving sexp, fields ~getters]

  let create (result : Result_.t) =
    let result = Result_.Private.to_struct result |> Resource.get_exn in
    { kind = Duckdb_stubs.duckdb_result_error_type (addr result)
    ; message = Duckdb_stubs.duckdb_result_error (addr result)
    }
  ;;

  let to_error t = Error.create_s [%sexp (t : t)]
end

let run conn query ~f =
  let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
  let result = Result_.create () in
  match
    Duckdb_stubs.duckdb_query
      !@conn
      query
      (Some (addr (Resource.get_exn (Result_.Private.to_struct result))))
  with
  | DuckDBError ->
    let error = Error.create result in
    Resource.free (Result_.Private.to_struct result) ~here:[%here];
    Error error
  | DuckDBSuccess ->
    protectx result ~f ~finally:(fun result ->
      Result_.Private.to_struct result |> Resource.free ~here:[%here])
    |> Ok
;;

let run' conn query = run conn query ~f:ignore

let run_exn conn query ~f =
  run conn query ~f |> Core.Result.map_error ~f:Error.to_error |> ok_exn
;;

let run_exn' conn query =
  run' conn query |> Core.Result.map_error ~f:Error.to_error |> ok_exn
;;

module Prepared = struct
  module Parameters = struct
    type t =
      | [] : t
      | ( :: ) : ('a Type.Typed_non_null.t * 'a) * t -> t

    let rec length = function
      | [] -> 0
      | _ :: t -> 1 + length t
    ;;
  end

  type t = Duckdb_stubs.Prepared_statement.t ptr Resource.t

  let bind (type a) t index (type_ : a Type.Typed_non_null.t) (value : a) =
    let index = Unsigned.UInt64.of_int index in
    match type_ with
    | Boolean -> Duckdb_stubs.duckdb_bind_boolean !@t index value
    | Tiny_int -> Duckdb_stubs.duckdb_bind_int8 !@t index value
    | Small_int -> Duckdb_stubs.duckdb_bind_int16 !@t index value
    | Integer -> Duckdb_stubs.duckdb_bind_int32 !@t index value
    | Big_int -> Duckdb_stubs.duckdb_bind_int64 !@t index value
    | U_tiny_int -> Duckdb_stubs.duckdb_bind_uint8 !@t index value
    | U_small_int -> Duckdb_stubs.duckdb_bind_uint16 !@t index value
    | U_integer -> Duckdb_stubs.duckdb_bind_uint32 !@t index value
    | U_big_int -> Duckdb_stubs.duckdb_bind_uint64 !@t index value
    | Float -> Duckdb_stubs.duckdb_bind_float !@t index value
    | Double -> Duckdb_stubs.duckdb_bind_double !@t index value
    | Timestamp -> Duckdb_stubs.duckdb_bind_timestamp !@t index value
    | Date -> Duckdb_stubs.duckdb_bind_date !@t index value
    | Time -> Duckdb_stubs.duckdb_bind_time !@t index value
    | Interval -> Duckdb_stubs.duckdb_bind_interval !@t index value
    | Huge_int -> Duckdb_stubs.duckdb_bind_hugeint !@t index value
    | Uhuge_int -> Duckdb_stubs.duckdb_bind_uhugeint !@t index value
    | Var_char -> Duckdb_stubs.duckdb_bind_varchar !@t index value
    | Blob ->
      Duckdb_stubs.duckdb_bind_blob
        !@t
        index
        (to_voidp (Ctypes_std_views.char_ptr_of_string value))
        (Unsigned.UInt64.of_int (String.length value))
    | Timestamp_s ->
      let duckdb_value =
        allocate Duckdb_stubs.Value.t (Value.create_non_null Timestamp_s value)
      in
      let result = Duckdb_stubs.duckdb_bind_value !@t index !@duckdb_value in
      Duckdb_stubs.duckdb_destroy_value duckdb_value;
      result
    | Timestamp_ms ->
      let duckdb_value =
        allocate Duckdb_stubs.Value.t (Value.create_non_null Timestamp_ms value)
      in
      let result = Duckdb_stubs.duckdb_bind_value !@t index !@duckdb_value in
      Duckdb_stubs.duckdb_destroy_value duckdb_value;
      result
    | Timestamp_ns ->
      let duckdb_value =
        allocate Duckdb_stubs.Value.t (Value.create_non_null Timestamp_ns value)
      in
      let result = Duckdb_stubs.duckdb_bind_value !@t index !@duckdb_value in
      Duckdb_stubs.duckdb_destroy_value duckdb_value;
      result
    | List child ->
      let duckdb_value =
        allocate Duckdb_stubs.Value.t (Value.create_non_null (List child) value)
      in
      let result = Duckdb_stubs.duckdb_bind_value !@t index !@duckdb_value in
      Duckdb_stubs.duckdb_destroy_value duckdb_value;
      result
  ;;

  let destroy = Resource.free

  let create conn query =
    let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
    let t =
      allocate
        Duckdb_stubs.Prepared_statement.t
        (from_voidp Duckdb_stubs.Prepared_statement.t_struct null)
    in
    match Duckdb_stubs.duckdb_prepare !@conn query t with
    | DuckDBSuccess ->
      Ok
        (Resource.create
           t
           ~name:"Duckdb.Prepared"
           ~free:Duckdb_stubs.duckdb_destroy_prepare)
    | DuckDBError ->
      let error = Duckdb_stubs.duckdb_prepare_error !@t in
      Duckdb_stubs.duckdb_destroy_prepare t;
      Error (Option.value_exn error)
  ;;

  let create_exn conn query =
    match create conn query with
    | Ok result -> result
    | Error msg -> failwith msg
  ;;

  let bind =
    let rec go t (parameters : Parameters.t) index =
      match parameters with
      | [] -> Ok ()
      | (type_, value) :: parameters ->
        (* TODO: check that the type matches up? *)
        (match bind t index type_ value with
         | DuckDBSuccess -> go t parameters (index + 1)
         | DuckDBError ->
           let error =
             Duckdb_stubs.duckdb_prepare_error !@t |> Option.value ~default:""
           in
           Error [%string "Failed to bind parameter at index %{index#Int}: %{error}"])
    in
    fun t (parameters : Parameters.t) ->
      let t = Resource.get_exn t in
      let num_parameters = Duckdb_stubs.duckdb_nparams !@t |> Unsigned.UInt64.to_int in
      let actual_parameters = Parameters.length parameters in
      match num_parameters = actual_parameters with
      | false ->
        Error
          [%string
            "Expected %{num_parameters#Int} parameters, got %{actual_parameters#Int}"]
      | true -> go t parameters 1
  ;;

  let bind_exn t parameters =
    match bind t parameters with
    | Ok () -> ()
    | Error msg -> failwith msg
  ;;

  let clear_bindings_exn t =
    let t = Resource.get_exn t in
    match Duckdb_stubs.duckdb_clear_bindings !@t with
    | DuckDBSuccess -> ()
    | DuckDBError ->
      let error = Duckdb_stubs.duckdb_prepare_error !@t in
      failwith (Option.value_exn error)
  ;;

  let run t ~f =
    let t = Resource.get_exn t in
    let result = Result_.create () in
    match
      Duckdb_stubs.duckdb_execute_prepared
        !@t
        (Some (addr (Resource.get_exn (Result_.Private.to_struct result))))
    with
    | DuckDBError ->
      let error = Error.create result in
      Resource.free (Result_.Private.to_struct result) ~here:[%here];
      Error error
    | DuckDBSuccess ->
      protectx result ~f ~finally:(fun result ->
        Result_.Private.to_struct result |> Resource.free ~here:[%here])
      |> Ok
  ;;

  let run' t = run t ~f:ignore
  let run_exn t ~f = run t ~f |> Core.Result.map_error ~f:Error.to_error |> ok_exn
  let run_exn' t = run' t |> Core.Result.map_error ~f:Error.to_error |> ok_exn
end
