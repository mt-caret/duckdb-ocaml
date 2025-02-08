#define DUCKDB_EXTENSION_NAME ocaml_extension_test

#include <duckdb_extension.h>
#include <caml/callback.h>

void RegisterAddNumbersFunction(duckdb_connection connection);

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
    // TODO: fixme
    char *argv[] = { "ocaml", "-init", "extension.ml", NULL };
    caml_startup(argv);

	// Register a demo function
	RegisterAddNumbersFunction(connection);

	// Return true to indicate succesful initialization
	return true;
}