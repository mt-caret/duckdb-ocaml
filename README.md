🦆❤️🐫

duckdb-ocaml provides bindings to the DuckDB C API. This is a work in progress.

## TODO

- [x] always associate destruction function with finalizer, since it's [safe](https://github.com/duckdb/duckdb/blob/0959644c1d57409e78d2fae0262f792921a54c55/src/main/capi/prepared-c.cpp#L390).
- [ ] set up CI
- [ ] support remaining logical types
- [ ] support conversion from and to OCaml types
- [x] support insertion
- [ ] PPX
- [x] bulk append API
- [ ] table functions
- [ ] replacement scans
- [x] set up dune-based build of duckdb
- [ ] add easy way to pretty-print result of query
