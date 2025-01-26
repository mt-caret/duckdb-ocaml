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
end
