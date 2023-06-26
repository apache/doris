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
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_system.h"
#include "io/hdfs_builder.h"
#include "util/jni-util.h"
#include "util/slice.h"

namespace doris::io {

class HdfsOpenReadBenchmark : public BaseBenchmark {
public:
    HdfsOpenReadBenchmark(int threads, int iterations, size_t file_size,
                          const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsReadBenchmark", threads, iterations, file_size, 3, conf_map) {}
    virtual ~HdfsOpenReadBenchmark() = default;

    Status init() override { return Status::OK(); }

    Status run(benchmark::State& state) override {
        std::shared_ptr<io::FileSystem> fs;
        io::FileReaderSPtr reader;
        bm_log("begin to init {}", _name);
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);

        auto file_path = get_file_path(state);
        RETURN_IF_ERROR(
                FileFactory::create_hdfs_reader(hdfs_params, file_path, &fs, &reader, reader_opts));
        bm_log("finish to init {}", _name);

        return read(state, reader);
    }
};

// Read a single specified file
class HdfsSingleReadBenchmark : public HdfsOpenReadBenchmark {
public:
    HdfsSingleReadBenchmark(int threads, int iterations, size_t file_size,
                            const std::map<std::string, std::string>& conf_map)
            : HdfsOpenReadBenchmark(threads, iterations, file_size, conf_map) {}
    virtual ~HdfsSingleReadBenchmark() = default;

    virtual std::string get_file_path(benchmark::State& state) override {
        std::string file_path = _conf_map["file_path"];
        bm_log("file_path: {}", file_path);
        return file_path;
    }
};

class HdfsCreateWriteBenchmark : public BaseBenchmark {
public:
    HdfsCreateWriteBenchmark(int threads, int iterations, size_t file_size,
                             const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsCreateWriteBenchmark", threads, iterations, file_size, 3,
                            conf_map) {}
    virtual ~HdfsCreateWriteBenchmark() = default;

    Status init() override { return Status::OK(); }

    Status run(benchmark::State& state) override {
        auto file_path = get_file_path(state);
        bm_log("file_path: {}", file_path);

        std::shared_ptr<io::HdfsFileSystem> fs;
        io::FileWriterPtr writer;
        THdfsParams hdfs_params = parse_properties(_conf_map);
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->create_file(file_path, &writer));
        return write(state, writer.get());
    }
};

class HdfsRenameBenchmark : public BaseBenchmark {
public:
    HdfsRenameBenchmark(int threads, int iterations, size_t file_size,
                        const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsRenameBenchmark", threads, 1, file_size, 1, conf_map) {}
    virtual ~HdfsRenameBenchmark() = default;

    Status init() override { return Status::OK(); }

    Status run(benchmark::State& state) override {
        bm_log("begin to run {}", _name);
        std::string base_dir = _conf_map["base_dir"];
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        auto new_file_path = fmt::format("{}/test_{}_new", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);

        auto start = std::chrono::high_resolution_clock::now();
        std::shared_ptr<io::HdfsFileSystem> fs;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->rename(file_path, new_file_path));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);

        state.counters["RenameCost"] =
                benchmark::Counter(1, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
        return Status::OK();
    }
};

class HdfsExistsBenchmark : public BaseBenchmark {
public:
    HdfsExistsBenchmark(int threads, int iterations, size_t file_size,
                        const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsExistsBenchmark", threads, iterations, file_size, 3, conf_map) {}
    virtual ~HdfsExistsBenchmark() = default;

    Status init() override { return Status::OK(); }

    Status run(benchmark::State& state) override {
        bm_log("begin to run {}", _name);
        std::string base_dir = _conf_map["base_dir"];
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);

        auto start = std::chrono::high_resolution_clock::now();
        std::shared_ptr<io::HdfsFileSystem> fs;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        bool res = false;
        RETURN_IF_ERROR(fs->exists(file_path, &res));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);

        state.counters["ExistsCost"] =
                benchmark::Counter(1, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
        return Status::OK();
    }
};

} // namespace doris::io
