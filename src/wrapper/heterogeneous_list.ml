open! Core

type 'a t =
  | [] : unit t
  | ( :: ) : 'a * 'b t -> ('a * 'b) t
