# Changelog

This file contains version of the third-party dependency libraries in the build-env image. The docker build-env image is apache/doris, and the tag is `build-env-${version}`

## 20240529

- Added: azure-core 1.10.3

## 20240325

- Modified: arrow 13.0.0 -> 15.0.2

## 20240112

- Modified: hadoop libhdfs 3.3.4.6

## v20231229

- Added: streamvbyte 1.0.0

## v20231210
- Modified: librdkafka 1.8.2 -> 2.0.2

## v20231020
- Added: avx2neon -> 1.0.0

## v20231008
- Modified: aws sdk 1.9.211 -> 1.11.119

## v20230824
- Modified: protobuf 3.15.0 -> 21.11
- Modified: glog 0.4.0 -> 0.6.0
- Modified: lz4 1.9.3 -> 1.9.4
- Modified: curl 7.79.0 -> 8.2.1
- Modified: zstd 1.5.2 -> 1.5.5
- Modified: arrow 7.0.0 -> 13.0.0
- Modified: abseil 20220623.1 -> 20230125.3
- Modified: orc 1.7.2 -> 1.9.0
- Modified: jemalloc for arrow 5.2.1 -> 5.3.0
- Modified: xsimd 7.0.0 -> 13.0.0
- Modified: opentelemetry-proto 0.19.0 -> 1.0.0
- Modified: opentelemetry 1.8.3 -> 1.10.0
- Added: c-ares -> 1.19.1
- Added: grpc -> 1.54.3

## v20230721

- Modified hadoop libhdfs 3.3.4.4 -> 3.3.4.5

## v20230625

- Modified benchmark 1.5.6 -> 1.8.0

## v20230531

- Modified hadoop libhdfs 3.3.4.2 -> 3.3.4.3
- Modified aws sdk 1.9.211 -> 1.9.272

## v20230525

- Removed:  clucene changed to submodule
- Modified: hadoop libhdfs 3.3.4.2

## v20230517
- Modified: opentelemetry-proto 0.18 -> 0.19 
- Modified: opentelemetry-cpp 1.4.0 -> 1.8.3

## v20230425
- Modified: clucene 2.4.9 -> 2.4.11

## v20230412
- Modified: sse2neon 1.5.1 -> 1.6.0
- Modified: libhdfs3 2.3.8 -> 2.3.9

## v20230411
- Modified: clucene 2.4.8 -> 2.4.9
- Modified: sse2neon 1.5.1 -> 1.6.0
- Modified: libhdfs3 2.3.8 -> 2.3.9

## v20230328
- Modified: brpc 1.2.0 -> 1.4.0
- Modified: boost 1.73.0 -> 1.81.0
- Modified: clucene 2.4.6 -> 2.4.8

## v20230228
- Modified: thrift 0.13 -> 0.16

## v20230221
- Modified: clucene 2.4.4 -> 2.4.6

## v20230218
- Modified: libhdfs3 2.3.7 -> 2.3.8
- Modified: jemalloc 5.2.1 -> 5.3.0

## v20230130
- Modified: libhdfs3 2.3.6 -> 2.3.7

## v20230120
- Modified: libhdfs3 2.3.5 -> 2.3.6

## v20230117
- Modified: bitshuffle 0.3.5 -> 0.5.1

## v20230112
- Added: clucene 2.4.4

## v20230111
- Fixed: libgsasl enable GSSAPI

## v20230110
- Modified: libhdfs3 2.3.4 -> 2.3.5

## v20230105
- Modified: openssl 1.1.1m -> 1.1.1s fix CVE-2022-1292

## v20221230
- Modified: libhdfs3 2.3.2 -> 2.3.4

## v20221228
- Modified: simdjson 1.0.2 -> 3.0.1

## v20221213
- Modified: protobuf 3.14.0 -> 3.15.0

## v20221212
- Modified: s2geometry 0.9.0 -> 0.10.0
- Added: Abseil, it is dependency of new s2geometry

## v20221209
- Modified: update parallel-hashmap from 1.33 to 1.3.8

## v20221015

- Modified: zstd 1.5.0 -> 1.5.2

## v20220914

- Added: xxhash 0.8.1
- Added: jemalloc 5.2.1, Build jemalloc separately, and name it as jemalloc_doris, to distinguish it from jemalloc in arrow.

## v20220811
- Modified: brpc 1.1.0 -> 1.2.0 fix _dl_sym undefined reference on Ubuntu22.04

## v20220802

- Modified: libhdfs3 2.3.1 -> 2.3.2

## v20220718

- Modified: brpc 1.0.0 -> 1.1.0
- Modified: leveldb 1.20 -> 1.23

## v20220606
- Added: vectorscan 5.4.7, and a patch for compilation

## v20220613
- Modified: update libhdfs3 from 2.3.0 to 2.3.1 fix client uuid set error

## v20220608
- Remove: remove libhdfs3 without kerberos support
- Modified: make libhdfs3 with kerberos support as default
- Modified: change libhdfs3 to https://github.com/yangzhg/libhdfs3/releases/tag/v2.3.0 . This version support arm CPUs

## v20220607
- Added: opentelemetry-cpp 1.4.0, it was introduced for tracing.
- Added: opentelemetry-proto 0.18.0, it is depended on by opentelemetry-cpp.
- Added: nlohmann/json 3.10.1, it is depended on by opentelemetry-cpp.

## v20220606
- Added: hyperscan 5.4.0, and a patch for compilation
- Added: ragel 6.1.0, it is used by hyperscan to generate files before compilation

## v20220522

- Added: libgsasl 1.8.0, this version of gsasl is only used for libhdfs3 with kerberos
- Added: krb5 1.19

Now there will be 2 set of libhdfs, one is without kerberos, the other is with kerberos, saved in `thirdparty/installed/libhdfs_with_kerberos/`

## v20220321
- Added: libbacktrace, it is used by boost stacktrace to print exception stack.

## v20220316
- Modified: CRoaring 0.3.4 -> 0.4.0

## v20220310
- Modified: arrow 5.0.0 -> 7.0.0
- Modified: aws-sdk-cpp 1.8.108 -> 1.9.211
- Modified: orc 1.6.6 -> 1.7.2

- Removed: aws-c-common: 0.4.63,aws-c-event-stream: 0.2.6, aws-checksums: 0.1.10, aws-c-io-0.7.0 aws-s2n: 0.10.0, aws-c-cal: 0.4.5; those libs are managed by aws-sdk-cpp now

## v20220211

- Added: simdjson 1.0.2

## v20211229

- Modified: OpenSSL with --with-rand-seed=devrandom
- Modified: brpc 1.0.0-rc02 -> 1.0.0

## v20211220

- Modified: OpenSSL 1.0.2k -> 1.1.1m
- Modified: cmake 3.19.8 -> 3.22.1
- Added: ccache

## v20211215

### Changes

- Added: cyrus-sasl
- Modified: librdkafka

## build-env-1.4.2

### Changes

- Added: breakpad

## build-env-1.4.1

### Changes

Openssl 1.1.1l introduces the getentropy() method, which requires glibc 2.25 or higher,
which will cause Doris BE to fail to run in the low-version glibc environment.
Temporarily roll back the openssl version.

- OpenSSL 1.1.1l -> 1.0.2k

## build-env-1.4.0

### Changes

- libevent -> 2.1.12
- OpenSSL 1.0.2k -> 1.1.1l
- thrift 0.9.3 -> 0.13.0
- protobuf 3.5.1 -> 3.14.0
- gflags 2.2.0 -> 2.2.2
- glog 0.3.3 -> 0.4.0
- googletest 1.8.0 -> 1.10.0
- snappy 1.1.7 -> 1.1.8
- gperftools 2.7 -> 2.9.1
- lz4 1.7.5 -> 1.9.3
- curl 7.54.1 -> 7.79.0
- re2 2017-05-01 -> 2021-02-02
- zstd 1.3.7 -> 1.5.0
- brotli 1.0.7 -> 1.0.9
- flatbuffers 1.10.0 -> 2.0.0
- apache-arrow 0.15.1 -> 5.0.0
- CRoaring 0.2.60 -> 0.3.4
- orc 1.5.8 -> 1.6.6
- libdivide 4.0.0 -> 5.0
- brpc 0.97 -> 1.0.0-rc02
- librdkafka 1.6.5 -> 1.8.0

### versions

- libevent-release: 2.1.12
- openssl: 1.1.1l
- thrift: 0.13.0
- protobuf: 3.14.0
- gflags: 2.2.2
- glog: 0.4.0
- googletest: 1.10.0
- snappy: 1.1.8
- gperftools: 2.9.1
- zlib: 1.2.11 
- lz4: 1.9.3
- bzip2: 1.0.8
- lzo: 2.10
- rapidjson: 1a803826f1197b5e30703afe4b9c0e7dd48074f5
- curl: 7.79.0
- re2: 2021-02-02
- boost: 1.73.0
- mysql-server: 5.7.18
- unixODBC: 2.3.7
- leveldb: 1.20
- incubator-brpc: 1.0.0-rc02
- rocksdb: 5.14.2
- librdkafka: 1.8.0
- zstd: 1.5.0
- brotli: 1.0.9
- flatbuffers: 2.0.0
- apache-arrow: 5.0.0
- s2geometry: 0.9.0
- bitshuffle: 0.3.5
- CRoaring: 0.3.4
- fmt: 7.1.3
- parallel-hashmap: 1.33
- orc: 1.6.6
- jemalloc: 5.2.1
- cctz: 2.3
- DataTables: 1.10.25
- aws-c-common: 0.4.63
- aws-c-event-stream: 0.2.6
- aws-checksums: 0.1.10
- aws-c-io-0.7.0
- aws-s2n: 0.10.0
- aws-c-cal: 0.4.5
- aws-sdk-cpp: 1.8.108
- liblzma: master
- libxml2: v2.9.10
- libgsasl: 1.10.0
- libhdfs3: master
- libdivide: 5.0
- pdqsort: git20180419
- benchmark: 1.5.6

## build_env-1.3.1

### versions
- libevent: 24236aed01798303745470e6c498bf606e88724a
- openssl: 1.0.2k
- thrift: 0.9.3
- cfe: 3.4.2.src
- protobuf: 3.5.1
- gflags: 2.2.0
- glog: 0.3.3
- googletest: 1.8.0
- snappy: 1.1.7
- gperftools: 2.7
- zlib: 1.2.11
- lz4: 1.7.5
- bzip2: 1.0.8
- lzo: 2.10
- rapidjson: 1a803826f1197b5e30703afe4b9c0e7dd48074f5
- curl: 7.54.1
- re2: 2017-05-01
- boost: 1.73.0
- mysql-server: 5.7.18
- unixODBC: 2.3.7
- leveldb: 1.20
- incubator-brpc: 0.9.7
- rocksdb: 5.14.2
- librdkafka: 1.6.1-RC3
- zstd: 1.3.7
- double-conversion: 3.1.1
- brotli: 1.0.7
- flatbuffers: 1.10.0
- arrow-apache: 0.15.1
- s2geometry: 0.9.0
- bitshuffle: 0.3.5
- CRoaring: 0.2.60
- fmt: 7.1.3
- parallel-hashmap: 1.33
- orc: 1.5.8
- jemalloc: 5.2.1
- cctz: 2.3
- DataTables: 1.10.25
- aws-c-common: 0.4.63
- aws-c-event-stream: 0.2.6
- aws-checksums: 0.1.10
- aws-c-io: 0.7.0
- s2n-tls: 0.10.0
- aws-c-cal: 0.4.5
- aws-sdk-cpp: 1.8.108
- liblzma: master
- libxml2: 2.9.10
- libgsasl: 1.10.0
- libhdfs3: master
- libdivide: 4.0.0
- pdqsort: git20180419
- benchmark: 1.5.6
