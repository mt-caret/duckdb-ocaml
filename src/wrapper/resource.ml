open! Core

module State = struct
  type 'a t =
    | Available of 'a
    | First_freed_at of Source_code_position.t
end

type 'a t =
  { mutable resource : 'a State.t
  ; name : string
  ; free : 'a -> unit
  }

let get_exn t =
  match t.resource with
  | First_freed_at first_freed_at ->
    raise_s [%message "Already freed" t.name (first_freed_at : Source_code_position.t)]
  | Available resource -> resource
;;

let free' t ~here =
  match t.resource with
  | First_freed_at _ -> ()
  | Available resource ->
    t.free resource;
    t.resource <- First_freed_at here
;;

let create resource ~name ~free =
  let t = { resource = Available resource; name; free } in
  Gc.Expert.add_finalizer_exn t (fun t -> free' t ~here:[%here]);
  t
;;

let free = free'
