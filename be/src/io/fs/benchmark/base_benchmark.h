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

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "util/slice.h"

namespace doris::io {

template <typename... Args>
void bm_log(const std::string& fmt, Args&&... args) {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm* local_time = std::localtime(&now_time);
    char time_str[20];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", local_time);
    std::cout << "[" << time_str << "] " << fmt::format(fmt, std::forward<Args>(args)...)
              << std::endl;
}

class BaseBenchmark {
public:
    BaseBenchmark(const std::string& name, int threads, int iterations, size_t file_size,
                  const std::map<std::string, std::string>& conf_map)
            : _name(name),
              _threads(threads),
              _iterations(iterations),
              _file_size(file_size),
              _conf_map(conf_map) {}
    virtual ~BaseBenchmark() = default;

    virtual Status init() { return Status::OK(); }
    virtual Status run(benchmark::State& state) { return Status::OK(); }

    void set_repetition(int rep) { _repetitions = rep; }

    void register_bm() {
        auto bm = benchmark::RegisterBenchmark(_name.c_str(), [&](benchmark::State& state) {
            Status st = this->init();
            if (!st.ok()) {
                bm_log("Benchmark {} init error: {}", _name, st.to_string());
                return;
            }
            for (auto _ : state) {
                st = this->run(state);
                if (!st.ok()) {
                    bm_log("Benchmark {} run error: {}", _name, st.to_string());
                    return;
                }
            }
        });
        if (_threads != 0) {
            bm->Threads(_threads);
        }
        if (_iterations != 0) {
            bm->Iterations(_iterations);
        }
        bm->Repetitions(_repetitions);

        bm->Unit(benchmark::kMillisecond);
        bm->UseManualTime();
        bm->ComputeStatistics("max", [](const std::vector<double>& v) -> double {
            return *(std::max_element(std::begin(v), std::end(v)));
        });
        bm->ComputeStatistics("min", [](const std::vector<double>& v) -> double {
            return *(std::min_element(std::begin(v), std::end(v)));
        });
    }

    virtual std::string get_file_path(benchmark::State& state) {
        std::string base_dir = _conf_map["base_dir"];
        std::string file_path;
        if (base_dir.ends_with("/")) {
            file_path = fmt::format("{}test_{}", base_dir, state.thread_index());
        } else {
            file_path = fmt::format("{}/test_{}", base_dir, state.thread_index());
        }
        bm_log("file_path: {}", file_path);
        return file_path;
    }

    Status read(benchmark::State& state, FileReaderSPtr reader) {
        bm_log("begin to read {}, thread: {}", _name, state.thread_index());
        size_t buffer_size =
                _conf_map.contains("buffer_size") ? std::stol(_conf_map["buffer_size"]) : 1000000L;
        std::vector<char> buffer;
        buffer.resize(buffer_size);
        doris::Slice data = {buffer.data(), buffer.size()};
        size_t offset = 0;
        size_t bytes_read = 0;

        size_t read_size = reader->size();
        if (_file_size > 0) {
            read_size = std::min(read_size, _file_size);
        }
        long remaining_size = read_size;

        Status status;
        auto start = std::chrono::high_resolution_clock::now();
        while (remaining_size > 0) {
            bytes_read = 0;
            size_t size = std::min(buffer_size, (size_t)remaining_size);
            data.size = size;
            status = reader->read_at(offset, data, &bytes_read);
            if (!status.ok()) {
                bm_log("reader read_at error: {}", status.to_string());
                break;
            }
            if (bytes_read == 0) { // EOF
                break;
            }
            offset += bytes_read;
            remaining_size -= bytes_read;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        state.SetIterationTime(elapsed_seconds.count());
        state.counters["ReadRate(B/S)"] =
                benchmark::Counter(read_size, benchmark::Counter::kIsRate);
        state.counters["ReadTotal(B)"] = read_size;
        state.counters["ReadTime(S)"] = elapsed_seconds.count();

        if (status.ok() && reader != nullptr) {
            status = reader->close();
        }
        bm_log("finish to read {}, thread: {}, size {}, seconds: {}, status: {}", _name,
               state.thread_index(), read_size, elapsed_seconds.count(), status);
        return status;
    }

    Status write(benchmark::State& state, FileWriter* writer) {
        bm_log("begin to write {}, thread: {}, size: {}", _name, state.thread_index(), _file_size);
        size_t write_size = _file_size;
        size_t buffer_size =
                _conf_map.contains("buffer_size") ? std::stol(_conf_map["buffer_size"]) : 1000000L;
        long remaining_size = write_size;
        std::vector<char> buffer;
        buffer.resize(buffer_size);
        doris::Slice data = {buffer.data(), buffer.size()};

        Status status;
        auto start = std::chrono::high_resolution_clock::now();
        while (remaining_size > 0) {
            size_t size = std::min(buffer_size, (size_t)remaining_size);
            data.size = size;
            status = writer->append(data);
            if (!status.ok()) {
                bm_log("writer append error: {}", status.to_string());
                break;
            }
            remaining_size -= size;
        }
        if (status.ok() && writer != nullptr && writer->state() != FileWriter::State::CLOSED) {
            status = writer->close();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds =
                std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        state.SetIterationTime(elapsed_seconds.count());
        state.counters["WriteRate(B/S)"] =
                benchmark::Counter(write_size, benchmark::Counter::kIsRate);
        state.counters["WriteTotal(B)"] = write_size;
        state.counters["WriteTime(S)"] = elapsed_seconds.count();

        bm_log("finish to write {}, thread: {}, size: {}, seconds: {}, status: {}", _name,
               state.thread_index(), write_size, elapsed_seconds.count(), status);
        return status;
    }

protected:
    std::string _name;
    int _threads;
    int _iterations;
    size_t _file_size;
    int _repetitions = 3;
    std::map<std::string, std::string> _conf_map;
};

} // namespace doris::io
