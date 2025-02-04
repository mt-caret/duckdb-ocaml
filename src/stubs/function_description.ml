open Ctypes
module Types = Types_generated

module Functions (F : FOREIGN) = struct
  open! F

  let duckdb_library_version = foreign "duckdb_library_version" (void @-> returning string)
  let duckdb_free = foreign "duckdb_free" (ptr void @-> returning void)

  let duckdb_open =
    foreign "duckdb_open" (string @-> ptr Types.Database.t @-> returning Types.State.t)
  ;;

  let duckdb_close = foreign "duckdb_close" (ptr Types.Database.t @-> returning void)

  let duckdb_connect =
    foreign
      "duckdb_connect"
      (Types.Database.t @-> ptr Types.Connection.t @-> returning Types.State.t)
  ;;

  let duckdb_disconnect =
    foreign "duckdb_disconnect" (ptr Types.Connection.t @-> returning void)
  ;;

  let duckdb_query =
    foreign
      "duckdb_query"
      (Types.Connection.t
       @-> string
       @-> ptr_opt Types.Result.t
       @-> returning Types.State.t)
  ;;

  let duckdb_destroy_result =
    foreign "duckdb_destroy_result" (ptr Types.Result.t @-> returning void)
  ;;

  let duckdb_fetch_chunk =
    foreign
      "duckdb_fetch_chunk"
      (Types.Result.t @-> returning (ptr_opt Types.Data_chunk.t_struct))
  ;;

  let duckdb_column_name =
    foreign "duckdb_column_name" (ptr Types.Result.t @-> Types.idx_t @-> returning string)
  ;;

  let duckdb_column_type =
    foreign
      "duckdb_column_type"
      (ptr Types.Result.t @-> Types.idx_t @-> returning Types.Type.t)
  ;;

  let duckdb_column_logical_type =
    foreign
      "duckdb_column_logical_type"
      (ptr Types.Result.t @-> Types.idx_t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_column_count =
    foreign "duckdb_column_count" (ptr Types.Result.t @-> returning Types.idx_t)
  ;;

  let duckdb_result_error =
    foreign "duckdb_result_error" (ptr Types.Result.t @-> returning string)
  ;;

  let duckdb_result_error_type =
    foreign
      "duckdb_result_error_type"
      (ptr Types.Result.t @-> returning Types.Error_type.t)
  ;;

  let duckdb_prepare =
    foreign
      "duckdb_prepare"
      (Types.Connection.t
       @-> string
       @-> ptr Types.Prepared_statement.t
       @-> returning Types.State.t)
  ;;

  let duckdb_destroy_prepare =
    foreign "duckdb_destroy_prepare" (ptr Types.Prepared_statement.t @-> returning void)
  ;;

  let duckdb_prepare_error =
    (* We can't use [string_opt] since the return type of the function is
       [const char*] instead of [char*]. *)
    foreign
      "duckdb_prepare_error"
      (Types.Prepared_statement.t @-> returning (ptr_opt (const char)))
  ;;

  let duckdb_nparams =
    foreign "duckdb_nparams" (Types.Prepared_statement.t @-> returning Types.idx_t)
  ;;

  let duckdb_parameter_name =
    foreign
      "duckdb_parameter_name"
      (Types.Prepared_statement.t @-> Types.idx_t @-> returning (ptr_opt (const char)))
  ;;

  let duckdb_param_type =
    foreign
      "duckdb_param_type"
      (Types.Prepared_statement.t @-> Types.idx_t @-> returning Types.Type.t)
  ;;

  let duckdb_clear_bindings =
    foreign
      "duckdb_clear_bindings"
      (Types.Prepared_statement.t @-> returning Types.State.t)
  ;;

  let duckdb_bind_boolean =
    foreign
      "duckdb_bind_boolean"
      (Types.Prepared_statement.t @-> Types.idx_t @-> bool @-> returning Types.State.t)
  ;;

  let duckdb_bind_int8 =
    foreign
      "duckdb_bind_int8"
      (Types.Prepared_statement.t @-> Types.idx_t @-> int8_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_int16 =
    foreign
      "duckdb_bind_int16"
      (Types.Prepared_statement.t @-> Types.idx_t @-> int16_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_int32 =
    foreign
      "duckdb_bind_int32"
      (Types.Prepared_statement.t @-> Types.idx_t @-> int32_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_int64 =
    foreign
      "duckdb_bind_int64"
      (Types.Prepared_statement.t @-> Types.idx_t @-> int64_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_uint8 =
    foreign
      "duckdb_bind_uint8"
      (Types.Prepared_statement.t @-> Types.idx_t @-> uint8_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_uint16 =
    foreign
      "duckdb_bind_uint16"
      (Types.Prepared_statement.t @-> Types.idx_t @-> uint16_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_uint32 =
    foreign
      "duckdb_bind_uint32"
      (Types.Prepared_statement.t @-> Types.idx_t @-> uint32_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_uint64 =
    foreign
      "duckdb_bind_uint64"
      (Types.Prepared_statement.t @-> Types.idx_t @-> uint64_t @-> returning Types.State.t)
  ;;

  let duckdb_bind_float =
    foreign
      "duckdb_bind_float"
      (Types.Prepared_statement.t @-> Types.idx_t @-> float @-> returning Types.State.t)
  ;;

  let duckdb_bind_double =
    foreign
      "duckdb_bind_double"
      (Types.Prepared_statement.t @-> Types.idx_t @-> double @-> returning Types.State.t)
  ;;

  let duckdb_execute_prepared =
    foreign
      "duckdb_execute_prepared"
      (Types.Prepared_statement.t @-> ptr_opt Types.Result.t @-> returning Types.State.t)
  ;;

  let duckdb_data_chunk_get_size =
    foreign "duckdb_data_chunk_get_size" (Types.Data_chunk.t @-> returning Types.idx_t)
  ;;

  let duckdb_data_chunk_get_vector =
    foreign
      "duckdb_data_chunk_get_vector"
      (Types.Data_chunk.t @-> Types.idx_t @-> returning Types.Vector.t)
  ;;

  let duckdb_vector_get_data =
    foreign "duckdb_vector_get_data" (Types.Vector.t @-> returning (ptr void))
  ;;

  let duckdb_vector_get_validity =
    foreign "duckdb_vector_get_validity" (Types.Vector.t @-> returning (ptr_opt uint64_t))
  ;;

  let duckdb_validity_row_is_valid =
    foreign
      "duckdb_validity_row_is_valid"
      (ptr uint64_t @-> Types.idx_t @-> returning bool)
  ;;

  let duckdb_destroy_data_chunk =
    foreign "duckdb_destroy_data_chunk" (ptr Types.Data_chunk.t @-> returning void)
  ;;

  let duckdb_get_type_id =
    foreign "duckdb_get_type_id" (Types.Logical_type.t @-> returning Types.Type.t)
  ;;

  let duckdb_list_type_child_type =
    foreign
      "duckdb_list_type_child_type"
      (Types.Logical_type.t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_array_type_child_type =
    foreign
      "duckdb_array_type_child_type"
      (Types.Logical_type.t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_array_type_array_size =
    foreign "duckdb_array_type_array_size" (Types.Logical_type.t @-> returning Types.idx_t)
  ;;

  let duckdb_map_type_key_type =
    foreign
      "duckdb_map_type_key_type"
      (Types.Logical_type.t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_map_type_value_type =
    foreign
      "duckdb_map_type_value_type"
      (Types.Logical_type.t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_struct_type_child_count =
    foreign
      "duckdb_struct_type_child_count"
      (Types.Logical_type.t @-> returning Types.idx_t)
  ;;

  let duckdb_struct_type_child_name =
    foreign
      "duckdb_struct_type_child_name"
      (Types.Logical_type.t @-> Types.idx_t @-> returning (ptr char))
  ;;

  let duckdb_struct_type_child_type =
    foreign
      "duckdb_struct_type_child_type"
      (Types.Logical_type.t @-> Types.idx_t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_union_type_member_count =
    foreign
      "duckdb_union_type_member_count"
      (Types.Logical_type.t @-> returning Types.idx_t)
  ;;

  let duckdb_union_type_member_name =
    foreign
      "duckdb_union_type_member_name"
      (Types.Logical_type.t @-> Types.idx_t @-> returning (ptr char))
  ;;

  let duckdb_union_type_member_type =
    foreign
      "duckdb_union_type_member_type"
      (Types.Logical_type.t @-> Types.idx_t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_destroy_logical_type =
    foreign "duckdb_destroy_logical_type" (ptr Types.Logical_type.t @-> returning void)
  ;;

  let duckdb_appender_create =
    foreign
      "duckdb_appender_create"
      (Types.Connection.t
       @-> string_opt
       @-> string
       @-> ptr Types.Appender.t
       @-> returning Types.State.t)
  ;;

  let duckdb_appender_column_count =
    foreign "duckdb_appender_column_count" (Types.Appender.t @-> returning Types.idx_t)
  ;;

  let duckdb_appender_column_type =
    foreign
      "duckdb_appender_column_type"
      (Types.Appender.t @-> Types.idx_t @-> returning Types.Logical_type.t)
  ;;

  let duckdb_appender_error =
    foreign "duckdb_appender_error" (Types.Appender.t @-> returning string_opt)
  ;;

  let duckdb_appender_flush =
    foreign "duckdb_appender_flush" (Types.Appender.t @-> returning Types.State.t)
  ;;

  let duckdb_appender_close =
    foreign "duckdb_appender_close" (Types.Appender.t @-> returning Types.State.t)
  ;;

  let duckdb_appender_destroy =
    foreign "duckdb_appender_destroy" (ptr Types.Appender.t @-> returning Types.State.t)
  ;;

  let duckdb_appender_end_row =
    foreign "duckdb_appender_end_row" (Types.Appender.t @-> returning Types.State.t)
  ;;

  let duckdb_append_bool =
    foreign "duckdb_append_bool" (Types.Appender.t @-> bool @-> returning Types.State.t)
  ;;

  let duckdb_append_int8 =
    foreign "duckdb_append_int8" (Types.Appender.t @-> int8_t @-> returning Types.State.t)
  ;;

  let duckdb_append_int16 =
    foreign
      "duckdb_append_int16"
      (Types.Appender.t @-> int16_t @-> returning Types.State.t)
  ;;

  let duckdb_append_int32 =
    foreign
      "duckdb_append_int32"
      (Types.Appender.t @-> int32_t @-> returning Types.State.t)
  ;;

  let duckdb_append_int64 =
    foreign
      "duckdb_append_int64"
      (Types.Appender.t @-> int64_t @-> returning Types.State.t)
  ;;

  let duckdb_append_uint8 =
    foreign
      "duckdb_append_uint8"
      (Types.Appender.t @-> uint8_t @-> returning Types.State.t)
  ;;

  let duckdb_append_uint16 =
    foreign
      "duckdb_append_uint16"
      (Types.Appender.t @-> uint16_t @-> returning Types.State.t)
  ;;

  let duckdb_append_uint32 =
    foreign
      "duckdb_append_uint32"
      (Types.Appender.t @-> uint32_t @-> returning Types.State.t)
  ;;

  let duckdb_append_uint64 =
    foreign
      "duckdb_append_uint64"
      (Types.Appender.t @-> uint64_t @-> returning Types.State.t)
  ;;

  let duckdb_append_float =
    foreign "duckdb_append_float" (Types.Appender.t @-> float @-> returning Types.State.t)
  ;;

  let duckdb_append_double =
    foreign
      "duckdb_append_double"
      (Types.Appender.t @-> double @-> returning Types.State.t)
  ;;
end
