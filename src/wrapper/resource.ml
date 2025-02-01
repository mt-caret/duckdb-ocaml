open! Core

type 'a t =
  { mutable resource : ('a, Source_code_position.t) result
  ; name : string
  ; free : 'a -> unit
  }

let create resource ~name ~free = { resource = Ok resource; name; free }

let get_exn t =
  match t.resource with
  | Error first_freed_at ->
    raise_s [%message "Already freed" t.name (first_freed_at : Source_code_position.t)]
  | Ok resource -> resource
;;

let free t ~here =
  match t.resource with
  | Error _ -> ()
  | Ok resource ->
    t.free resource;
    t.resource <- Error here
;;
