# Native MongoDB client dependencies

This module prepares the Java dependencies for the native MongoDB connector. It
bundles the official synchronous driver, its core, BSON, and BSON record codec
at one version, using a module-local BOM so other connector plugins keep their
own dependencies. The archive also carries the Snappy and Zstandard Java providers
so FE metadata reads accept the same compression URI options as the native BE.
The FE host supplies logging APIs and bindings.

The dependency-only archive does not register a `ConnectorProvider` and cannot
create a MongoDB catalog. The FE build stages it as
`plugins/connector/mongodb.zip`, without unpacking it into an auto-loaded plugin
directory. A later connector implementation will add the provider and enable the
normal plugin deployment path.

The BE uses the MongoDB C Driver (`libmongoc` and `libbson`) built together by:

```sh
bash thirdparty/build-thirdparty.sh mongo_c_driver
```

The native build produces PIC static archives with OpenSSL, Cyrus SASL, SRV DNS,
Snappy, zstd and zlib support. It reuses Doris-managed dependencies and the driver's
bundled utf8proc for SCRAM-SHA-256. It does not require the C++ driver, a BE JVM,
Reactive Streams, or client-side encryption libraries. The reader and BSON
`DataTypeSerDe` implementation are separate changes. The BE CMake targets `bson2`
and `mongoc2` expose the static headers, definitions and dependency closure; they
are not added to the common BE link set until a reader consumes them.
