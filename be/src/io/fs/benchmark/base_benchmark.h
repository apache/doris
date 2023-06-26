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
                  int repetitions, const std::map<std::string, std::string>& conf_map)
            : _name(name),
              _threads(threads),
              _iterations(iterations),
              _file_size(file_size),
              _repetitions(repetitions),
              _conf_map(conf_map) {}
    virtual ~BaseBenchmark() = default;

    virtual Status init() { return Status::OK(); }
    virtual Status run(benchmark::State& state) { return Status::OK(); }

    void register_bm() {
        auto bm = benchmark::RegisterBenchmark(_name.c_str(), [&](benchmark::State& state) {
            Status st;
            if (state.thread_index() == 0) {
                st = this->init();
            }
            if (st != Status::OK()) {
                bm_log("Benchmark {} init error: {}", _name, st.to_string());
                return;
            }
            for (auto _ : state) {
                st = this->run(state);
                if (st != Status::OK()) {
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

protected:
    std::string _name;
    int _threads;
    int _iterations;
    size_t _file_size;
    int _repetitions = 1;
    std::map<std::string, std::string> _conf_map;
};

} // namespace doris::io
