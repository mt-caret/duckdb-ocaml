open! Core
open Ctypes
include C.Functions
include C.Types

let char_ptr_to_string_with_duckdb_free ~f =
  let str_ptr = f () in
  let str = Ctypes_std_views.string_of_char_ptr str_ptr in
  duckdb_free (to_voidp str_ptr);
  str
;;

let duckdb_struct_type_child_name =
  fun logical_type index ->
  char_ptr_to_string_with_duckdb_free ~f:(fun () ->
    duckdb_struct_type_child_name logical_type index)
;;

let duckdb_union_type_member_name =
  fun logical_type index ->
  char_ptr_to_string_with_duckdb_free ~f:(fun () ->
    duckdb_union_type_member_name logical_type index)
;;
