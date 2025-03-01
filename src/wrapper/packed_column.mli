open! Core

type t =
  | T : 'a Type.Typed_non_null.t * 'a array -> t
  | T_opt : 'a Type.Typed.t * 'a option array -> t
