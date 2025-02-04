open! Core

module Error : sig
  type t [@@deriving sexp]

  val message : t -> string
  val to_error : t -> Error.t
end

module Result : sig
  type t

  val column_count : t -> int
  val schema : t -> (string * Type.t) array

  module Private : sig
    val to_struct : t -> Duckdb_stubs.Result.t Ctypes.structure
  end
end

val run : Connection.t -> string -> f:(Result.t -> 'a) -> ('a, Error.t) result
val run' : Connection.t -> string -> (unit, Error.t) result
val run_exn : Connection.t -> string -> f:(Result.t -> 'a) -> 'a
val run_exn' : Connection.t -> string -> unit

module Prepared : sig
  module Parameters : sig
    type t =
      | [] : t
      | ( :: ) : ('a Type.Typed.t * 'a) * t -> t
  end

  type t

  val create : Connection.t -> string -> (t, string) result
  val destroy : t -> here:Source_code_position.t -> unit
  val bind : t -> Parameters.t -> (unit, string) result
  val clear_bindings_exn : t -> unit
  val run : t -> f:(Result.t -> 'a) -> ('a, Error.t) result
  val run' : t -> (unit, Error.t) result
  val run_exn : t -> f:(Result.t -> 'a) -> 'a
  val run_exn' : t -> unit
end
