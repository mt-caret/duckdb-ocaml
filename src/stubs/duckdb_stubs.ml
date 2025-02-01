open! Core
open Ctypes
include C.Functions
include C.Types

let duckdb_prepare_error prepared_statement =
  duckdb_prepare_error prepared_statement
  |> Option.map ~f:Ctypes_std_views.string_of_char_ptr
;;

let char_ptr_to_string_with_duckdb_free str_ptr =
  let str = Ctypes_std_views.string_of_char_ptr str_ptr in
  duckdb_free (to_voidp str_ptr);
  str
;;

let duckdb_parameter_name =
  fun prepared_statement index ->
  duckdb_parameter_name prepared_statement index
  |> Option.map ~f:char_ptr_to_string_with_duckdb_free
;;

let duckdb_struct_type_child_name =
  fun logical_type index ->
  duckdb_struct_type_child_name logical_type index |> char_ptr_to_string_with_duckdb_free
;;

let duckdb_union_type_member_name =
  fun logical_type index ->
  duckdb_union_type_member_name logical_type index |> char_ptr_to_string_with_duckdb_free
;;
