(lang dune 3.17)

(name duckdb)

(using ctypes 0.3)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Author Name <author@example.com>")

(maintainers "Maintainer Name <maintainer@example.com>")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name duckdb)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends
  (ocaml (>= 5.0.0))
  (dune (>= 3.17))
  core
  ctypes
  ctypes-foreign
  integers
  textutils
  (base_quickcheck :with-test)
  (expect_test_helpers_core :with-test)
  (ppx_jane :with-test))
 (tags
  ("add topics" "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
