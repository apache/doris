// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "io/file_factory.h"
#include "io/fs/benchmark/base_benchmark.h"
#include "io/fs/file_writer.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "util/s3_uri.h"
#include "util/slice.h"

namespace doris::io {

class S3Benchmark : public BaseBenchmark {
public:
    S3Benchmark(const std::string& name, int threads, int iterations, size_t file_size,
                const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark(name, threads, iterations, file_size, 3, conf_map) {}
    virtual ~S3Benchmark() = default;

    Status init() override {
        bm_log("begin to init {}", _name);
        S3URI s3_uri(_conf_map["file"]);
        RETURN_IF_ERROR(s3_uri.parse());
        RETURN_IF_ERROR(
                S3ClientFactory::convert_properties_to_s3_conf(_conf_map, s3_uri, &_s3_conf));
        RETURN_IF_ERROR(io::S3FileSystem::create(std::move(_s3_conf), "", &_fs));
        RETURN_IF_ERROR(init_other());
        bm_log("finish to init {}", _name);
        return Status::OK();
    }

    virtual Status init_other() { return Status::OK(); }

protected:
    doris::S3Conf _s3_conf;
    std::shared_ptr<io::S3FileSystem> _fs;
};

class S3ReadBenchmark : public S3Benchmark {
public:
    S3ReadBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3ReadBenchmark", iterations, conf_map), _result(buffer, 128) {}
    virtual ~S3ReadBenchmark() = default;

    Status init_other() override {
        std::string file_path = _conf_map["file"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        RETURN_IF_ERROR(FileFactory::create_s3_reader(
                _conf_map, file_path, reinterpret_cast<std::shared_ptr<io::FileSystem>*>(&_fs),
                &_reader, reader_opts));
        return Status::OK();
    }

    Status run() override { return _reader->read_at(0, _result, &_bytes_read); }

private:
    doris::S3Conf _s3_conf;
    io::FileReaderSPtr _reader;
    char buffer[128];
    doris::Slice _result;
    size_t _bytes_read = 0;
};

class S3SizeBenchmark : public S3Benchmark {
public:
    S3SizeBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3SizeBenchmark", iterations, conf_map) {}
    virtual ~S3SizeBenchmark() = default;

    Status run() override {
        int64_t file_size = 0;
        return _fs->file_size(_conf_map["file"], &file_size);
    }
};

class S3ListBenchmark : public S3Benchmark {
public:
    S3ListBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3ListBenchmark", iterations, conf_map) {}
    virtual ~S3ListBenchmark() = default;

    Status run() override {
        std::vector<FileInfo> files;
        bool exists = true;
        return _fs->list(_conf_map["file"], true, &files, &exists);
    }
};

class S3OpenBenchmark : public S3Benchmark {
public:
    S3OpenBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3OpenBenchmark", iterations, conf_map) {}
    virtual ~S3OpenBenchmark() = default;

    Status init_other() override {
        std::string file_path = _conf_map["file"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        RETURN_IF_ERROR(FileFactory::create_s3_reader(
                _conf_map, file_path, reinterpret_cast<std::shared_ptr<io::FileSystem>*>(&_fs),
                &_reader, reader_opts));
        return Status::OK();
    }

    Status run() override { return _fs->open_file(_conf_map["file"], &_reader); }

private:
    io::FileReaderSPtr _reader;
};

class S3ConnectBenchmark : public S3Benchmark {
public:
    S3ConnectBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3ConnectBenchmark", iterations, conf_map) {}
    virtual ~S3ConnectBenchmark() = default;

    Status run() override { return _fs->connect(); }
};

} // namespace doris::io
