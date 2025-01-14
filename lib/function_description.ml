open Ctypes
module Types = Types_generated

module Functions (F : FOREIGN) = struct
  open! F

  let duckdb_library_version = foreign "duckdb_library_version" (void @-> returning string)

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
end
