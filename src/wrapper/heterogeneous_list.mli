(** A list containing values of arbitrary types. *)

open! Core

type 'a t =
  | [] : Nothing.t t
  | ( :: ) : 'a * 'b t -> ('a * 'b) t
