open! Core

type t =
  | T_non_null : 'a Type.Typed_non_null.t * 'a array -> t
  | T : 'a Type.Typed.t * 'a option array -> t
