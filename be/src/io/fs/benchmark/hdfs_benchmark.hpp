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
        std::string base_dir = _conf_map["baseDir"];
        size_t buffer_size =
                _conf_map.contains("buffer_size") ? std::stol(_conf_map["buffer_size"]) : 1000000L;
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);
        RETURN_IF_ERROR(
                FileFactory::create_hdfs_reader(hdfs_params, file_path, &fs, &reader, reader_opts));
        bm_log("finish to init {}", _name);

        bm_log("begin to run {}", _name);
        Status status;
        std::vector<char> buffer;
        buffer.resize(buffer_size);
        doris::Slice data = {buffer.data(), buffer.size()};
        size_t offset = 0;
        size_t bytes_read = 0;

        auto start = std::chrono::high_resolution_clock::now();
        size_t read_size = _file_size;
        long remaining_size = read_size;

        while (remaining_size > 0) {
            bytes_read = 0;
            size_t size = std::min(buffer_size, (size_t)remaining_size);
            data.size = size;
            status = reader->read_at(offset, data, &bytes_read);
            if (status != Status::OK() || bytes_read < 0) {
                bm_log("reader read_at error: {}", status.to_string());
                break;
            }
            if (bytes_read == 0) { // EOF
                break;
            }
            offset += bytes_read;
            remaining_size -= bytes_read;
        }
        bm_log("finish to run {}", _name);

        auto end = std::chrono::high_resolution_clock::now();

        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());

        if (reader != nullptr) {
            reader->close();
        }
        return status;
    }
};

class HdfsOpenBenchmark : public BaseBenchmark {
public:
    HdfsOpenBenchmark(int threads, int iterations, size_t file_size,
                      const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsOpenBenchmark", threads, iterations, file_size, 3, conf_map) {}
    virtual ~HdfsOpenBenchmark() = default;

    Status init() override { return Status::OK(); }

    Status run(benchmark::State& state) override {
        bm_log("begin to run {}", _name);
        auto start = std::chrono::high_resolution_clock::now();
        std::string base_dir = _conf_map["baseDir"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);
        std::shared_ptr<io::HdfsFileSystem> fs;
        io::FileReaderSPtr reader;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->open_file(file_path, reader_opts, &reader));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);

        if (reader != nullptr) {
            reader->close();
        }
        return Status::OK();
    }

private:
};

class HdfsCreateWriteBenchmark : public BaseBenchmark {
public:
    HdfsCreateWriteBenchmark(int threads, int iterations, size_t file_size,
                             const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsCreateWriteBenchmark", threads, iterations, file_size, 3,
                            conf_map) {}
    virtual ~HdfsCreateWriteBenchmark() = default;

    Status init() override {
        std::string base_dir = _conf_map["baseDir"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        std::shared_ptr<io::HdfsFileSystem> fs;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->delete_directory(base_dir));
        return Status::OK();
    }

    Status run(benchmark::State& state) override {
        bm_log("begin to run {}", _name);
        auto start = std::chrono::high_resolution_clock::now();
        std::string base_dir = _conf_map["baseDir"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);
        std::shared_ptr<io::HdfsFileSystem> fs;
        io::FileWriterPtr writer;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->create_file(file_path, &writer));
        Status status;
        size_t write_size = _file_size;
        size_t buffer_size =
                _conf_map.contains("buffer_size") ? std::stol(_conf_map["buffer_size"]) : 1000000L;
        long remaining_size = write_size;
        std::vector<char> buffer;
        buffer.resize(buffer_size);
        doris::Slice data = {buffer.data(), buffer.size()};
        while (remaining_size > 0) {
            size_t size = std::min(buffer_size, (size_t)remaining_size);
            data.size = size;
            status = writer->append(data);
            if (status != Status::OK()) {
                bm_log("writer append error: {}", status.to_string());
                break;
            }
            remaining_size -= size;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);

        if (writer != nullptr) {
            writer->close();
        }
        return status;
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
        auto start = std::chrono::high_resolution_clock::now();
        std::string base_dir = _conf_map["baseDir"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        auto new_file_path = fmt::format("{}/test_{}_new", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);
        std::shared_ptr<io::HdfsFileSystem> fs;
        io::FileWriterPtr writer;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->rename(file_path, new_file_path));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);

        if (writer != nullptr) {
            writer->close();
        }
        return Status::OK();
    }

private:
};

class HdfsDeleteBenchmark : public BaseBenchmark {
public:
    HdfsDeleteBenchmark(int threads, int iterations, size_t file_size,
                        const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("HdfsDeleteBenchmark", threads, 1, file_size, 1, conf_map) {}
    virtual ~HdfsDeleteBenchmark() = default;

    Status init() override { return Status::OK(); }

    Status run(benchmark::State& state) override {
        bm_log("begin to run {}", _name);
        auto start = std::chrono::high_resolution_clock::now();
        std::string base_dir = _conf_map["baseDir"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);
        std::shared_ptr<io::HdfsFileSystem> fs;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        RETURN_IF_ERROR(fs->delete_file(file_path));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);
        return Status::OK();
    }

private:
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
        auto start = std::chrono::high_resolution_clock::now();
        std::string base_dir = _conf_map["baseDir"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        THdfsParams hdfs_params = parse_properties(_conf_map);
        auto file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        bm_log("file_path: {}", file_path);
        std::shared_ptr<io::HdfsFileSystem> fs;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", &fs));
        bool res = false;
        RETURN_IF_ERROR(fs->exists(file_path, &res));
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        bm_log("finish to run {}", _name);
        return Status::OK();
    }
};

} // namespace doris::io
