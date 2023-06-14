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

#include <iostream>
#include <string>
#include <vector>

#include "io/fs/benchmark/s3_benchmark.hpp"

namespace doris::io {

class BenchmarkFactory {
public:
    static Status getBm(const std::string fs_type, const std::string op_type, int64_t iterations,
                        const std::map<std::string, std::string>& conf_map, BaseBenchmark** bm);
};

Status BenchmarkFactory::getBm(const std::string fs_type, const std::string op_type,
                               int64_t iterations,
                               const std::map<std::string, std::string>& conf_map,
                               BaseBenchmark** bm) {
    if (fs_type == "s3") {
        if (op_type == "read") {
            *bm = new S3ReadBenchmark(iterations, conf_map);
        } else {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "unknown params: fs_type: {}, op_type: {}, iterations: {}", fs_type, op_type,
                    iterations);
        }
    }
    return Status::OK();
}

class MultiBenchmark {
public:
    MultiBenchmark(const std::string& type, const std::string& operation, int64_t iterations,
                   const std::map<std::string, std::string>& conf_map)
            : _type(type), _operation(operation), _iterations(iterations), _conf_map(conf_map) {}

    ~MultiBenchmark() {
        for (auto bm : benchmarks) {
            delete bm;
        }
    }

    Status init_env() { return Status::OK(); }

    Status init_bms() {
        BaseBenchmark* bm;
        Status st = BenchmarkFactory::getBm(_type, _operation, _iterations, _conf_map, &bm);
        if (!st) {
            return st;
        }
        bm->register_bm();
        benchmarks.emplace_back(bm);
        return Status::OK();
    }

private:
    std::vector<BaseBenchmark*> benchmarks;
    std::string _type;
    std::string _operation;
    int64_t _iterations;
    std::map<std::string, std::string> _conf_map;
};

} // namespace doris::io
