open Ctypes
module Types = Types_generated

module Functions (F : FOREIGN) = struct
  open! F

  let duckdb_library_version = foreign "duckdb_library_version" (void @-> returning string)
  let duckdb_free = foreign "duckdb_free" (ptr void @-> returning void)

  let duckdb_open =
    foreign
      "duckdb_open"
      (string @-> ptr Types.duckdb_database @-> returning Types.duckdb_state)
  ;;

  let duckdb_close = foreign "duckdb_close" (ptr Types.duckdb_database @-> returning void)

  let duckdb_connect =
    foreign
      "duckdb_connect"
      (Types.duckdb_database
       @-> ptr Types.duckdb_connection
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_disconnect =
    foreign "duckdb_disconnect" (ptr Types.duckdb_connection @-> returning void)
  ;;

  let duckdb_query =
    foreign
      "duckdb_query"
      (Types.duckdb_connection
       @-> string
       @-> ptr_opt Types.duckdb_result
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_destroy_result =
    foreign "duckdb_destroy_result" (ptr Types.duckdb_result @-> returning void)
  ;;

  let duckdb_fetch_chunk =
    foreign
      "duckdb_fetch_chunk"
      (Types.duckdb_result @-> returning (ptr_opt Types.duckdb_data_chunk_struct))
  ;;

  let duckdb_column_name =
    foreign
      "duckdb_column_name"
      (ptr Types.duckdb_result @-> Types.idx_t @-> returning string)
  ;;

  let duckdb_column_type =
    foreign
      "duckdb_column_type"
      (ptr Types.duckdb_result @-> Types.idx_t @-> returning Types.duckdb_type)
  ;;

  let duckdb_column_logical_type =
    foreign
      "duckdb_column_logical_type"
      (ptr Types.duckdb_result @-> Types.idx_t @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_column_count =
    foreign "duckdb_column_count" (ptr Types.duckdb_result @-> returning Types.idx_t)
  ;;

  let duckdb_result_error =
    foreign "duckdb_result_error" (ptr Types.duckdb_result @-> returning string)
  ;;

  let duckdb_result_error_type =
    foreign
      "duckdb_result_error_type"
      (ptr Types.duckdb_result @-> returning Types.duckdb_error_type)
  ;;

  let duckdb_prepare =
    foreign
      "duckdb_prepare"
      (Types.duckdb_connection
       @-> string
       @-> ptr Types.duckdb_prepared_statement
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_destroy_prepare =
    foreign
      "duckdb_destroy_prepare"
      (ptr Types.duckdb_prepared_statement @-> returning void)
  ;;

  let duckdb_prepare_error =
    (* We can't use [string_opt] since the return type of the function is
       [const char*] instead of [char*]. *)
    foreign
      "duckdb_prepare_error"
      (Types.duckdb_prepared_statement @-> returning (ptr_opt (const char)))
  ;;

  let duckdb_nparams =
    foreign "duckdb_nparams" (Types.duckdb_prepared_statement @-> returning Types.idx_t)
  ;;

  let duckdb_parameter_name =
    foreign
      "duckdb_parameter_name"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> returning (ptr_opt (const char)))
  ;;

  let duckdb_param_type =
    foreign
      "duckdb_param_type"
      (Types.duckdb_prepared_statement @-> Types.idx_t @-> returning Types.duckdb_type)
  ;;

  let duckdb_clear_bindings =
    foreign
      "duckdb_clear_bindings"
      (Types.duckdb_prepared_statement @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_boolean =
    foreign
      "duckdb_bind_boolean"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> bool
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_int8 =
    foreign
      "duckdb_bind_int8"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> int8_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_int16 =
    foreign
      "duckdb_bind_int16"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> int16_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_int32 =
    foreign
      "duckdb_bind_int32"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> int32_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_int64 =
    foreign
      "duckdb_bind_int64"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> int64_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_uint8 =
    foreign
      "duckdb_bind_uint8"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> uint8_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_uint16 =
    foreign
      "duckdb_bind_uint16"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> uint16_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_uint32 =
    foreign
      "duckdb_bind_uint32"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> uint32_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_uint64 =
    foreign
      "duckdb_bind_uint64"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> uint64_t
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_float =
    foreign
      "duckdb_bind_float"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> float
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_bind_double =
    foreign
      "duckdb_bind_double"
      (Types.duckdb_prepared_statement
       @-> Types.idx_t
       @-> double
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_execute_prepared =
    foreign
      "duckdb_execute_prepared"
      (Types.duckdb_prepared_statement
       @-> ptr_opt Types.duckdb_result
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_data_chunk_get_size =
    foreign
      "duckdb_data_chunk_get_size"
      (Types.duckdb_data_chunk @-> returning Types.idx_t)
  ;;

  let duckdb_data_chunk_get_vector =
    foreign
      "duckdb_data_chunk_get_vector"
      (Types.duckdb_data_chunk @-> Types.idx_t @-> returning Types.duckdb_vector)
  ;;

  let duckdb_vector_get_data =
    foreign "duckdb_vector_get_data" (Types.duckdb_vector @-> returning (ptr void))
  ;;

  let duckdb_vector_get_validity =
    foreign
      "duckdb_vector_get_validity"
      (Types.duckdb_vector @-> returning (ptr_opt uint64_t))
  ;;

  let duckdb_validity_row_is_valid =
    foreign
      "duckdb_validity_row_is_valid"
      (ptr uint64_t @-> Types.idx_t @-> returning bool)
  ;;

  let duckdb_destroy_data_chunk =
    foreign "duckdb_destroy_data_chunk" (ptr Types.duckdb_data_chunk @-> returning void)
  ;;

  let duckdb_get_type_id =
    foreign
      "duckdb_get_type_id"
      (Types.duckdb_logical_type @-> returning Types.duckdb_type)
  ;;

  let duckdb_list_type_child_type =
    foreign
      "duckdb_list_type_child_type"
      (Types.duckdb_logical_type @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_array_type_child_type =
    foreign
      "duckdb_array_type_child_type"
      (Types.duckdb_logical_type @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_array_type_array_size =
    foreign
      "duckdb_array_type_array_size"
      (Types.duckdb_logical_type @-> returning Types.idx_t)
  ;;

  let duckdb_map_type_key_type =
    foreign
      "duckdb_map_type_key_type"
      (Types.duckdb_logical_type @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_map_type_value_type =
    foreign
      "duckdb_map_type_value_type"
      (Types.duckdb_logical_type @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_struct_type_child_count =
    foreign
      "duckdb_struct_type_child_count"
      (Types.duckdb_logical_type @-> returning Types.idx_t)
  ;;

  let duckdb_struct_type_child_name =
    foreign
      "duckdb_struct_type_child_name"
      (Types.duckdb_logical_type @-> Types.idx_t @-> returning (ptr char))
  ;;

  let duckdb_struct_type_child_type =
    foreign
      "duckdb_struct_type_child_type"
      (Types.duckdb_logical_type @-> Types.idx_t @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_union_type_member_count =
    foreign
      "duckdb_union_type_member_count"
      (Types.duckdb_logical_type @-> returning Types.idx_t)
  ;;

  let duckdb_union_type_member_name =
    foreign
      "duckdb_union_type_member_name"
      (Types.duckdb_logical_type @-> Types.idx_t @-> returning (ptr char))
  ;;

  let duckdb_union_type_member_type =
    foreign
      "duckdb_union_type_member_type"
      (Types.duckdb_logical_type @-> Types.idx_t @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_destroy_logical_type =
    foreign
      "duckdb_destroy_logical_type"
      (ptr Types.duckdb_logical_type @-> returning void)
  ;;

  let duckdb_appender_create =
    foreign
      "duckdb_appender_create"
      (Types.duckdb_connection
       @-> string_opt
       @-> string
       @-> ptr Types.duckdb_appender
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_appender_column_count =
    foreign
      "duckdb_appender_column_count"
      (Types.duckdb_appender @-> returning Types.idx_t)
  ;;

  let duckdb_appender_column_type =
    foreign
      "duckdb_appender_column_type"
      (Types.duckdb_appender @-> Types.idx_t @-> returning Types.duckdb_logical_type)
  ;;

  let duckdb_appender_error =
    foreign "duckdb_appender_error" (Types.duckdb_appender @-> returning string_opt)
  ;;

  let duckdb_appender_flush =
    foreign
      "duckdb_appender_flush"
      (Types.duckdb_appender @-> returning Types.duckdb_state)
  ;;

  let duckdb_appender_close =
    foreign
      "duckdb_appender_close"
      (Types.duckdb_appender @-> returning Types.duckdb_state)
  ;;

  let duckdb_appender_destroy =
    foreign
      "duckdb_appender_destroy"
      (ptr Types.duckdb_appender @-> returning Types.duckdb_state)
  ;;

  let duckdb_appender_end_row =
    foreign
      "duckdb_appender_end_row"
      (Types.duckdb_appender @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_bool =
    foreign
      "duckdb_append_bool"
      (Types.duckdb_appender @-> bool @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_int8 =
    foreign
      "duckdb_append_int8"
      (Types.duckdb_appender @-> int8_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_int16 =
    foreign
      "duckdb_append_int16"
      (Types.duckdb_appender @-> int16_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_int32 =
    foreign
      "duckdb_append_int32"
      (Types.duckdb_appender @-> int32_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_int64 =
    foreign
      "duckdb_append_int64"
      (Types.duckdb_appender @-> int64_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_uint8 =
    foreign
      "duckdb_append_uint8"
      (Types.duckdb_appender @-> uint8_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_uint16 =
    foreign
      "duckdb_append_uint16"
      (Types.duckdb_appender @-> uint16_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_uint32 =
    foreign
      "duckdb_append_uint32"
      (Types.duckdb_appender @-> uint32_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_uint64 =
    foreign
      "duckdb_append_uint64"
      (Types.duckdb_appender @-> uint64_t @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_float =
    foreign
      "duckdb_append_float"
      (Types.duckdb_appender @-> float @-> returning Types.duckdb_state)
  ;;

  let duckdb_append_double =
    foreign
      "duckdb_append_double"
      (Types.duckdb_appender @-> double @-> returning Types.duckdb_state)
  ;;
end
