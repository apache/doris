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

#include "io/fs/benchmark/hdfs_benchmark.hpp"
#include "io/fs/benchmark/s3_benchmark.hpp"

namespace doris::io {

class BenchmarkFactory {
public:
    static Status getBm(const std::string fs_type, const std::string op_type, int64_t threads,
                        int64_t iterations, size_t file_size,
                        const std::map<std::string, std::string>& conf_map, BaseBenchmark** bm);
};

Status BenchmarkFactory::getBm(const std::string fs_type, const std::string op_type,
                               int64_t threads, int64_t iterations, size_t file_size,
                               const std::map<std::string, std::string>& conf_map,
                               BaseBenchmark** bm) {
    if (fs_type == "s3") {
        if (op_type == "create_write") {
            *bm = new S3CreateWriteBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "open_read") {
            *bm = new S3OpenReadBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "single_read") {
            *bm = new S3SingleReadBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "prefetch_read") {
            *bm = new S3PrefetchReadBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "rename") {
            *bm = new S3RenameBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "exists") {
            *bm = new S3ExistsBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "list") {
            *bm = new S3ListBenchmark(threads, iterations, file_size, conf_map);
        } else {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "unknown params: fs_type: {}, op_type: {}, iterations: {}", fs_type, op_type,
                    iterations);
        }
    } else if (fs_type == "hdfs") {
        if (op_type == "create_write") {
            *bm = new HdfsCreateWriteBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "open_read") {
            *bm = new HdfsOpenReadBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "single_read") {
            *bm = new HdfsSingleReadBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "rename") {
            *bm = new HdfsRenameBenchmark(threads, iterations, file_size, conf_map);
        } else if (op_type == "exists") {
            *bm = new HdfsExistsBenchmark(threads, iterations, file_size, conf_map);
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
    MultiBenchmark(const std::string& type, const std::string& operation, int64_t threads,
                   int64_t iterations, size_t file_size,
                   const std::map<std::string, std::string>& conf_map)
            : _type(type),
              _operation(operation),
              _threads(threads),
              _iterations(iterations),
              _file_size(file_size),
              _conf_map(conf_map) {}

    ~MultiBenchmark() {
        for (auto bm : _benchmarks) {
            delete bm;
        }
    }

    Status init_env() {
        std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
        if (!doris::config::init(conffile.c_str(), true, true, true)) {
            return Status::Error<INTERNAL_ERROR>("error read config file.");
        }
        doris::CpuInfo::init();
        Status status = Status::OK();
        if (doris::config::enable_java_support) {
            // Init jni
            status = doris::JniUtil::Init();
            if (!status.ok()) {
                LOG(WARNING) << "Failed to initialize JNI: " << status;
                exit(1);
            }
        }

        return Status::OK();
    }

    Status init_bms() {
        BaseBenchmark* bm;
        Status st = BenchmarkFactory::getBm(_type, _operation, _threads, _iterations, _file_size,
                                            _conf_map, &bm);
        if (!st) {
            return st;
        }
        bm->register_bm();
        _benchmarks.emplace_back(bm);
        return Status::OK();
    }

private:
    std::vector<BaseBenchmark*> _benchmarks;
    std::string _type;
    std::string _operation;
    int64_t _threads;
    int64_t _iterations;
    size_t _file_size;
    std::map<std::string, std::string> _conf_map;
};

} // namespace doris::io
