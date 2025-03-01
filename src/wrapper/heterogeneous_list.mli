(** A list containing values of arbitrary types. *)

open! Core

type 'a t =
  | [] : unit t
  | ( :: ) : 'a * 'b t -> ('a * 'b) t
