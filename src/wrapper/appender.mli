open! Core

type t

val create : Connection.t -> ?schema:string -> string -> t
val close : t -> here:Source_code_position.t -> (unit, string) result
val close_exn : t -> here:Source_code_position.t -> unit
val flush : t -> here:Source_code_position.t -> (unit, string) result
val flush_exn : t -> here:Source_code_position.t -> unit
val column_types : t -> Type.t list

(* TODO: add a version that takes an ['a option Heterogeneous_list.t]. *)
val append
  :  t
  -> 'a Type.Typed_non_null.List.t
  -> 'a Heterogeneous_list.t list
  -> (unit, string) result

val append_exn
  :  t
  -> 'a Type.Typed_non_null.List.t
  -> 'a Heterogeneous_list.t list
  -> unit
