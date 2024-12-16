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
#include "io/fs/buffered_reader.h"
#include "io/fs/file_system.h"
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
            : BaseBenchmark(name, threads, iterations, file_size, conf_map) {}
    virtual ~S3Benchmark() = default;

    Status get_fs(const std::string& path, std::shared_ptr<io::S3FileSystem>* fs) {
        S3URI s3_uri(path);
        RETURN_IF_ERROR(s3_uri.parse());
        S3Conf s3_conf;
        RETURN_IF_ERROR(
                S3ClientFactory::convert_properties_to_s3_conf(_conf_map, s3_uri, &s3_conf));
        *fs = DORIS_TRY(io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID));
        return Status::OK();
    }
};

class S3OpenReadBenchmark : public S3Benchmark {
public:
    S3OpenReadBenchmark(int threads, int iterations, size_t file_size,
                        const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3ReadBenchmark", threads, iterations, file_size, conf_map) {}
    virtual ~S3OpenReadBenchmark() = default;

    virtual void set_default_file_size() {
        if (_file_size <= 0) {
            _file_size = 10 * 1024 * 1024; // default 10MB
        }
    }

    Status run(benchmark::State& state) override {
        auto file_path = get_file_path(state);
        std::shared_ptr<io::S3FileSystem> fs;
        RETURN_IF_ERROR(get_fs(file_path, &fs));

        io::FileReaderSPtr reader;
        io::FileReaderOptions reader_opts;
        FileDescription fd;
        RETURN_IF_ERROR(fs->open_file(file_path, &reader, &reader_opts));
        return read(state, reader);
    }
};

// Read a single specified file
class S3SingleReadBenchmark : public S3OpenReadBenchmark {
public:
    S3SingleReadBenchmark(int threads, int iterations, size_t file_size,
                          const std::map<std::string, std::string>& conf_map)
            : S3OpenReadBenchmark(threads, iterations, file_size, conf_map) {}
    virtual ~S3SingleReadBenchmark() = default;

    virtual void set_default_file_size() override {}

    virtual std::string get_file_path(benchmark::State& state) override {
        std::string file_path = _conf_map["file_path"];
        bm_log("file_path: {}", file_path);
        return file_path;
    }
};

// Read a single specified file by prefetch reader
class S3PrefetchReadBenchmark : public S3Benchmark {
public:
    S3PrefetchReadBenchmark(int threads, int iterations, size_t file_size,
                            const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3PrefetchReadBenchmark", threads, iterations, file_size, conf_map) {}
    virtual ~S3PrefetchReadBenchmark() = default;

    virtual std::string get_file_path(benchmark::State& state) override {
        std::string file_path = _conf_map["file_path"];
        bm_log("file_path: {}", file_path);
        return file_path;
    }

    Status run(benchmark::State& state) override {
        FileSystemProperties fs_props;
        fs_props.system_type = TFileType::FILE_S3;
        fs_props.properties = _conf_map;

        FileDescription fd;
        fd.path = get_file_path(state);
        fd.file_size = _file_size;
        io::FileReaderOptions reader_options;
        IOContext io_ctx;
        auto reader = DORIS_TRY(io::DelegateReader::create_file_reader(
                nullptr, fs_props, fd, reader_options, io::DelegateReader::AccessMode::SEQUENTIAL,
                &io_ctx));
        return read(state, reader);
    }
};

class S3CreateWriteBenchmark : public S3Benchmark {
public:
    S3CreateWriteBenchmark(int threads, int iterations, size_t file_size,
                           const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3CreateWriteBenchmark", threads, iterations, file_size, conf_map) {}
    virtual ~S3CreateWriteBenchmark() = default;

    Status run(benchmark::State& state) override {
        auto file_path = get_file_path(state);
        if (_file_size <= 0) {
            _file_size = 10 * 1024 * 1024; // default 10MB
        }
        std::shared_ptr<io::S3FileSystem> fs;
        RETURN_IF_ERROR(get_fs(file_path, &fs));

        io::FileWriterPtr writer;
        RETURN_IF_ERROR(fs->create_file(file_path, &writer));
        return write(state, writer.get());
    }
};

class S3ListBenchmark : public S3Benchmark {
public:
    S3ListBenchmark(int threads, int iterations, size_t file_size,
                    const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3ListBenchmark", threads, iterations, file_size, conf_map) {}
    virtual ~S3ListBenchmark() = default;

    virtual std::string get_file_path(benchmark::State& state) override {
        return _conf_map["base_dir"];
    }

    Status run(benchmark::State& state) override {
        auto file_path = get_file_path(state);
        std::shared_ptr<io::S3FileSystem> fs;
        RETURN_IF_ERROR(get_fs(file_path, &fs));

        auto start = std::chrono::high_resolution_clock::now();
        std::vector<FileInfo> files;
        bool exists = true;
        RETURN_IF_ERROR(fs->list(file_path, true, &files, &exists));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        state.SetIterationTime(elapsed_seconds.count());
        state.counters["ListCost"] =
                benchmark::Counter(1, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);

        std::stringstream ss;
        int i = 0;
        for (auto& file_info : files) {
            if (i > 2) {
                break;
            }
            ++i;
            ss << "[" << file_info.file_name << ", " << file_info.file_size << ", "
               << file_info.is_file << "] ";
        }
        bm_log("list files: {}", ss.str());

        return Status::OK();
    }
};

class S3RenameBenchmark : public S3Benchmark {
public:
    S3RenameBenchmark(int threads, int iterations, size_t file_size,
                      const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3RenameBenchmark", threads, iterations, file_size, conf_map) {
        // rename can only do once
        set_repetition(1);
    }

    virtual ~S3RenameBenchmark() = default;

    Status run(benchmark::State& state) override {
        auto file_path = get_file_path(state);
        auto new_file_path = file_path + "_new";
        std::shared_ptr<io::S3FileSystem> fs;
        RETURN_IF_ERROR(get_fs(file_path, &fs));

        auto start = std::chrono::high_resolution_clock::now();
        RETURN_IF_ERROR(fs->rename(file_path, new_file_path));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        state.SetIterationTime(elapsed_seconds.count());
        state.counters["RenameCost"] =
                benchmark::Counter(1, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);

        return Status::OK();
    }
};

class S3ExistsBenchmark : public S3Benchmark {
public:
    S3ExistsBenchmark(int threads, int iterations, size_t file_size,
                      const std::map<std::string, std::string>& conf_map)
            : S3Benchmark("S3ExistsBenchmark", threads, iterations, file_size, conf_map) {}
    virtual ~S3ExistsBenchmark() = default;

    Status run(benchmark::State& state) override {
        auto file_path = get_file_path(state);
        std::shared_ptr<io::S3FileSystem> fs;
        RETURN_IF_ERROR(get_fs(file_path, &fs));

        auto start = std::chrono::high_resolution_clock::now();
        bool res = false;
        RETURN_IF_ERROR(fs->exists(file_path, &res));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        state.SetIterationTime(elapsed_seconds.count());
        state.counters["ExistsCost"] =
                benchmark::Counter(1, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);

        return Status::OK();
    }
};

} // namespace doris::io
