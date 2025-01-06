open Ctypes
module Types = Types_generated

module Functions (F : FOREIGN) = struct
  open! F

  let duckdb_library_version = foreign "duckdb_library_version" (void @-> returning string)

  let duckdb_open =
    foreign
      "duckdb_open"
      (string @-> ptr Types.duckdb_database @-> returning Types.duckdb_state)
  ;;

  let duckdb_close = foreign "duckdb_close" (ptr Types.duckdb_database @-> returning void)

  let duckdb_connect =
    foreign
      "duckdb_connect"
      (Types.duckdb_database
       @-> ptr Types.duckdb_connection
       @-> returning Types.duckdb_state)
  ;;

  let duckdb_disconnect =
    foreign "duckdb_disconnect" (ptr Types.duckdb_connection @-> returning void)
  ;;
end
