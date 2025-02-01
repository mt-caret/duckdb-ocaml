
```
CAML_LD_LIBRARY_PATH="/absolute/path/to/libduckdb:$CAML_LD_LIBRARY_PATH" dune build -w @default @fmt @runtest
```

TODO:

- [x] always associate destruction function with finalizer, since it's [safe](https://github.com/duckdb/duckdb/blob/0959644c1d57409e78d2fae0262f792921a54c55/src/main/capi/prepared-c.cpp#L390).
- [ ] set up CI
- [ ] support remaining logical types
- [ ] support conversion from and to OCaml types
- [x] support insertion
- [ ] PPX
- [ ] bulk append API
- [ ] table functions
- [ ] replacement scans
- [ ] set up dune-based build of duckdb?
