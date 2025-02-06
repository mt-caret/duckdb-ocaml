open! Core
open! Ctypes

module Error = struct
  type t =
    { kind : Duckdb_stubs.Error_type.t
    ; message : string
    }
  [@@deriving sexp, fields ~getters]

  let to_error t = Error.create_s [%sexp (t : t)]
end

module Result = struct
  type t =
    { result : Duckdb_stubs.Result.t structure
    ; schema : (string * Type.t) array
    }
  [@@deriving fields ~getters]

  let create result =
    let schema =
      Duckdb_stubs.duckdb_column_count (addr result)
      |> Unsigned.UInt64.to_int
      |> Array.init ~f:(fun i ->
        let i = Unsigned.UInt64.of_int i in
        let name = Duckdb_stubs.duckdb_column_name (addr result) i in
        let type_ =
          Duckdb_stubs.duckdb_column_logical_type (addr result) i
          |> Type.Private.with_logical_type ~f:Type.of_logical_type_exn
        in
        name, type_)
    in
    { result; schema }
  ;;

  let column_count t = Array.length t.schema

  module Private = struct
    let to_struct t = t.result
  end
end

let run conn query ~f =
  let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
  let duckdb_result = make Duckdb_stubs.Result.t in
  match Duckdb_stubs.duckdb_query !@conn query (Some (addr duckdb_result)) with
  | DuckDBError ->
    let error : Error.t =
      { kind = Duckdb_stubs.duckdb_result_error_type (addr duckdb_result)
      ; message = Duckdb_stubs.duckdb_result_error (addr duckdb_result)
      }
    in
    Duckdb_stubs.duckdb_destroy_result (addr duckdb_result);
    Error error
  | DuckDBSuccess ->
    let result = Result.create duckdb_result in
    protectx result ~f ~finally:(fun result ->
      Duckdb_stubs.duckdb_destroy_result (addr result.result))
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
      | ( :: ) : ('a Type.Typed.t * 'a) * t -> t

    let rec length = function
      | [] -> 0
      | _ :: t -> 1 + length t
    ;;
  end

  type t = Duckdb_stubs.Prepared_statement.t ptr Resource.t

  let bind (type a) t index (type_ : a Type.Typed.t) (value : a) =
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
    | Decimal -> Duckdb_stubs.duckdb_bind_decimal !@t index value
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
    let duckdb_result = make Duckdb_stubs.Result.t in
    match Duckdb_stubs.duckdb_execute_prepared !@t (Some (addr duckdb_result)) with
    | DuckDBError ->
      let error : Error.t =
        { kind = Duckdb_stubs.duckdb_result_error_type (addr duckdb_result)
        ; message = Duckdb_stubs.duckdb_result_error (addr duckdb_result)
        }
      in
      Duckdb_stubs.duckdb_destroy_result (addr duckdb_result);
      Error error
    | DuckDBSuccess ->
      let result = Result.create duckdb_result in
      protectx result ~f ~finally:(fun result ->
        Duckdb_stubs.duckdb_destroy_result (addr result.result))
      |> Ok
  ;;

  let run' t = run t ~f:ignore
  let run_exn t ~f = run t ~f |> Core.Result.map_error ~f:Error.to_error |> ok_exn
  let run_exn' t = run' t |> Core.Result.map_error ~f:Error.to_error |> ok_exn
end
