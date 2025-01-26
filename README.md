
```
CAML_LD_LIBRARY_PATH="/absolute/path/to/libduckdb:$CAML_LD_LIBRARY_PATH" dune build -w @default @fmt @runtest
```

TODO:

- [ ] set up CI
- [ ] support remaining logical types
- [ ] support conversion from and to OCaml types
- [ ] support insertion
- [ ] PPX
- [ ] bulk append API
- [ ] table functions
- [ ] replacement scans
- [ ] set up dune-based build of duckdb?