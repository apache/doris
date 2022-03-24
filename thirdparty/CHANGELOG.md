# Changelog

This file contains version of the third-party dependency libraries in the build-env image. The docker build-env image is apache/incubator-doris, and the tag is `build-env-${version}`

## v20220321
- Add libbacktrace, it is used by boost stacktrace to print exception stack.

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
