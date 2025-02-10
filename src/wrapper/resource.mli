(** [Resource] is a wrapper around some data that must be freed when it is no
    longer needed. *)

open! Core

type 'a t

val create : 'a -> name:string -> free:('a -> unit) -> 'a t

(** Raises an exception if the resource has already been freed. 

    When [t] is garbage collected, a finalizer will call [free] to avoid memory
    leaks. As a result, callers of [get_exn] should not (e.g. store the returned
    value in a record) since the underlying resource may be freed at any time. *)
val get_exn : 'a t -> 'a

val free : 'a t -> here:Source_code_position.t -> unit
