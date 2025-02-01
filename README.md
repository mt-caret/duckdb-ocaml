🦆❤️🐫

duckdb-ocaml provides bindings to the DuckDB C API. This is a work in progress.

## how to build

Download [the v.1.1.3 release of duckdb zip](https://duckdb.org/docs/installation/index?version=stable&environment=cplusplus&platform=linux&download_method=direct&architecture=x86_64),
and unpack the contents into the root of this repository with name `libduckdb`:

```
$ tree ./libduckdb/
./libduckdb/
├── duckdb.h
├── duckdb.hpp
├── libduckdb.so
└── libduckdb_static.a
```

Then, run the following command to build the library and run tests:

```
CAML_LD_LIBRARY_PATH="/absolute/path/to/libduckdb:$CAML_LD_LIBRARY_PATH" dune build -w @default @fmt @runtest
```

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
- [ ] set up dune-based build of duckdb
