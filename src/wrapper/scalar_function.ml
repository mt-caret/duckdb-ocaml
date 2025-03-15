open! Core
open! Ctypes

module Signature = struct
  type ('a, 'ret) t =
    | Returning : 'ret Type.Typed_non_null.t -> ('ret, 'ret) t
    | ( :: ) : 'a Type.Typed_non_null.t * ('b, 'ret) t -> ('a -> 'b, 'ret) t

  let rec returning_type : type a b. (a, b) t -> b Type.Typed_non_null.t = function
    | Returning type_ -> type_
    | _ :: rest -> returning_type rest
  ;;

  let rec to_untyped : type a b. (a, b) t -> _ list = function
    | Returning x -> [ Type.Typed_non_null.to_untyped x ]
    | x :: xs -> Type.Typed_non_null.to_untyped x :: to_untyped xs
  ;;
end

module Argument_arrays = struct
  type ('a, 'ret) t =
    | Returning : ('ret, 'ret) t
    | ( :: ) : 'a array * ('b, 'ret) t -> ('a -> 'b, 'ret) t

  let create =
    let rec go : type a b. (a, b) Signature.t -> Data_chunk.t -> int -> (a, b) t =
      fun type_signature data_chunk i ->
      match type_signature with
      | Returning _ -> Returning
      | type_ :: rest ->
        Data_chunk.get_exn data_chunk type_ i :: go rest data_chunk (i + 1)
    in
    fun type_signature data_chunk -> go type_signature data_chunk 0
  ;;

  let rec apply : type a b. (a, b) t -> int -> f:a -> b =
    fun t index ~f ->
    match t with
    | Returning -> f
    | ( :: ) (array, rest) -> apply rest index ~f:(f (Array.get array index))
  ;;
end

module F =
  (val Foreign.dynamic_funptr
         ~runtime_lock:true
         (Duckdb_stubs.Function_info.t
          @-> Duckdb_stubs.Data_chunk.t
          @-> Duckdb_stubs.Vector.t
          @-> returning void))

let create_expert_exn name types ~f ~conn =
  let scalar_function =
    allocate
      Duckdb_stubs.Scalar_function.t
      (Duckdb_stubs.duckdb_create_scalar_function ())
  in
  let f =
    F.of_fun (fun info chunk output ->
      try f info chunk output with
      | exn ->
        let error =
          Exn.to_string_mach exn
          ^
          match Backtrace.Exn.most_recent_for_exn exn with
          | Some backtrace -> [%string "\n%{backtrace#Backtrace}"]
          | None -> ""
        in
        Duckdb_stubs.duckdb_scalar_function_set_error info error)
  in
  Duckdb_stubs.duckdb_scalar_function_set_name !@scalar_function name;
  List.iteri types ~f:(fun i t ->
    Type.to_logical_type t
    |> Type.Private.with_logical_type ~f:(fun logical_type ->
      if i = List.length types - 1
      then
        Duckdb_stubs.duckdb_scalar_function_set_return_type !@scalar_function logical_type
      else
        Duckdb_stubs.duckdb_scalar_function_add_parameter !@scalar_function logical_type));
  Duckdb_stubs.duckdb_scalar_function_set_function
    !@scalar_function
    (Ctypes.coerce F.t Duckdb_stubs.Scalar_function.function_ f);
  let status =
    Duckdb_stubs.duckdb_register_scalar_function
      !@(Connection.Private.to_ptr conn |> Resource.get_exn)
      !@scalar_function
  in
  Duckdb_stubs.duckdb_destroy_scalar_function scalar_function;
  match status with
  | DuckDBSuccess ->
    let closure =
      Resource.create f ~name:"Duckdb.Function.Scalar" ~free:(fun f -> F.free f)
    in
    let database = Connection.Private.database conn in
    Database.Private.add_closure_root closure database
  | DuckDBError ->
    F.free f;
    failwith "Failed to register scalar function"
;;

let create_exn (type a b) name (type_signature : (a, b) Signature.t) ~(f : a) ~conn =
  let return_type = Signature.returning_type type_signature in
  create_expert_exn
    name
    (Signature.to_untyped type_signature)
    ~f:(fun _info chunk output ->
      let chunk = Data_chunk.Private.create_do_not_free chunk in
      let argument_arrays = Argument_arrays.create type_signature chunk in
      Array.init (Data_chunk.length chunk) ~f:(fun i ->
        Argument_arrays.apply argument_arrays i ~f)
      |> Vector.set_array output return_type)
    ~conn
;;

module Expert = struct
  let create_exn = create_expert_exn
end
