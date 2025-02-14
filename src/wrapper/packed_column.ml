open! Core

type t =
  | T : 'a Type.Typed.t * 'a array -> t
  | T_opt : 'a Type.Typed.t * 'a option array -> t
