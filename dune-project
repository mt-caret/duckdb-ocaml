(lang dune 3.17)

(name duckdb)

(using ctypes 0.3)

(generate_opam_files true)

(source
 (github mt-caret/duckdb-ocaml))

(authors mtakeda)

(maintainers mtakeda)

(license MIT)

(documentation https://github.com/mt-caret/duckdb-ocaml)

(package
 (name duckdb)
 (synopsis "OCaml bindings to the DuckDB C API")
 (description "OCaml bindings to the DuckDB C API")
 (depends
  (ocaml
   (>= 5.0.0))
  (dune
   (>= 3.17))
  core
  ctypes
  ctypes-foreign
  integers
  textutils
  (base_quickcheck :with-test)
  (expect_test_helpers_core :with-test)
  ppx_jane)
 (tags
  ("database" "sql" "duckdb")))
