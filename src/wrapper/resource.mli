(** [Resource] is a wrapper around some data that must be freed when it is no
    longer needed. *)

open! Core

type 'a t

val create : 'a -> name:string -> free:('a -> unit) -> 'a t

(** Raises an exception if the resource has already been freed. *)
val get_exn : 'a t -> 'a

val free : 'a t -> here:Source_code_position.t -> unit
