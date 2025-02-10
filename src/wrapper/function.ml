open! Core
open! Ctypes

module Scalar = struct
  module F =
    (val Foreign.dynamic_funptr
           ~runtime_lock:true
           (Duckdb_stubs.Function_info.t
            @-> Duckdb_stubs.Data_chunk.t
            @-> Duckdb_stubs.Vector.t
            @-> returning void))

  type t' =
    { scalar_function : Duckdb_stubs.Scalar_function.t ptr
    ; f : F.t
    }

  type t = t' Resource.t

  let create name types ~f =
    let scalar_function =
      allocate
        Duckdb_stubs.Scalar_function.t
        (Duckdb_stubs.duckdb_create_scalar_function ())
    in
    let f = F.of_fun f in
    Duckdb_stubs.duckdb_scalar_function_set_name !@scalar_function name;
    List.iteri types ~f:(fun i t ->
      Type.to_logical_type t
      |> Type.Private.with_logical_type ~f:(fun logical_type ->
        if i = List.length types - 1
        then
          Duckdb_stubs.duckdb_scalar_function_set_return_type
            !@scalar_function
            logical_type
        else
          Duckdb_stubs.duckdb_scalar_function_add_parameter !@scalar_function logical_type));
    Duckdb_stubs.duckdb_scalar_function_set_function
      !@scalar_function
      (Ctypes.coerce F.t Duckdb_stubs.Scalar_function.function_ f);
    Resource.create
      { scalar_function; f }
      ~name:"Duckdb.Function.Scalar"
      ~free:(fun { scalar_function; f } ->
        Duckdb_stubs.duckdb_destroy_scalar_function scalar_function;
        F.free f)
  ;;

  let register_exn t conn =
    let t = Resource.get_exn t in
    let conn = Connection.Private.to_ptr conn |> Resource.get_exn in
    match Duckdb_stubs.duckdb_register_scalar_function !@conn !@(t.scalar_function) with
    | DuckDBSuccess -> ()
    | DuckDBError -> failwith "Failed to register scalar function"
  ;;
end
