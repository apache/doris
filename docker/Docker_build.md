## Doris Develop Environment based on docker
### 1. Build docker image

```aidl
cd docker

docker build -t doris:v1.0  .

-- doris is docker image repository name and base is tag name , you can change them to what you like

```

### 2. Use docker image

```aidl
docker run -it --name doris  doris:v1.0
// if you want to build your local code ,you should execute:
docker run -it --name test -v **/incubator-doris:/var/local/incubator-doris  doris:v1.0

// then build source code
cd /var/local/incubator-doris

sh build.sh   -- build project

//execute unit test
sh run-ut.sh --run  -- run unit test

```

### 3. Thirdparty list

The detail list of thirdparty we mirrored as following:

* [libevent-20180622-24236aed01798303745470e6c498bf606e88724a.zip](http://palo-opensource.gz.bcebos.com/libevent-20180622-24236aed01798303745470e6c498bf606e88724a.zip?authorization=bce-auth-v1%2F069fc2786e464e63a5f1183824ddb522%2F2018-06-22T02%3A37%3A48Z%2F-1%2Fhost%2F031b0cc42ab83ca4e0ec3608cba963e95c1ddc46fc70a14457323e2d7960e6ef)
* [openssl-1.0.2k.tar.gz](https://www.openssl.org/source/openssl-1.0.2k.tar.gz)
* [thrift-0.9.3.tar.gz](http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz)
* [llvm-3.4.2.src.tar.gz](http://releases.llvm.org/3.4.2/llvm-3.4.2.src.tar.gz)
* [cfe-3.4.2.src.tar.gz](http://releases.llvm.org/3.4.2/cfe-3.4.2.src.tar.gz)
* [compiler-rt-5.0.0.src.tar.xz](http://releases.llvm.org/5.0.0/compiler-rt-5.0.0.src.tar.xz)
* [protobuf-3.5.1.tar.gz](https://github.com/google/protobuf/archive/v3.5.1.tar.gz)
* [gflags-2.2.0.tar.gz](https://github.com/gflags/gflags/archive/v2.2.0.tar.gz)
* [glog-0.3.3.tar.gz](https://github.com/google/glog/archive/v0.3.3.tar.gz)
* [googletest-release-1.8.0.tar.gz](https://github.com/google/googletest/archive/release-1.8.0.tar.gz)
* [snappy-1.1.4.tar.gz](https://github.com/google/snappy/releases/download/1.1.4/snappy-1.1.4.tar.gz)
* [gperftools-2.7.tar.gz](https://github.com/gperftools/gperftools/archive/gperftools-2.7.tar.gz)
* [zlib-1.2.11.tar.gz](https://sourceforge.net/projects/libpng/files/zlib/1.2.11/zlib-1.2.11.tar.gz)
* [lz4-1.7.5.tar.gz](https://github.com/lz4/lz4/archive/v1.7.5.tar.gz)
* [bzip2-1.0.6.tar.gz](https://fossies.org/linux/misc/bzip2-1.0.6.tar.gz)
* [lzo-2.10.tar.gz](http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz)
* [rapidjson-1.1.0.tar.gz](https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz)
* [curl-7.54.0.tar.gz](https://curl.haxx.se/download/curl-7.54.0.tar.gz)
* [re2-2017-05-01.tar.gz](https://github.com/google/re2/archive/2017-05-01.tar.gz)
* [boost_1_64_0.tar.gz](https://dl.bintray.com/boostorg/release/1.64.0/source/boost_1_64_0.tar.gz)
* [mysql-5.7.18.tar.gz](https://github.com/mysql/mysql-server/archive/mysql-5.7.18.tar.gz)
* [boost_1_59_0.tar.gz](http://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz)
* [leveldb-1.20.tar.gz](https://github.com/google/leveldb/archive/v1.20.tar.gz)
* [brpc-0.9.0.tar.gz](https://github.com/brpc/brpc/archive/v0.9.0.tar.gz)
* [rocksdb-5.14.2.tar.gz](https://github.com/facebook/rocksdb/archive/v5.14.2.tar.gz)
* [librdkafka-0.11.6-RC5.tar.gz](https://github.com/edenhill/librdkafka/archive/v0.11.6-RC5.tar.gz)
