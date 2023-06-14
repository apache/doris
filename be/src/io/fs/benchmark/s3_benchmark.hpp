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
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "util/slice.h"

namespace doris::io {

class S3ReadBenchmark : public BaseBenchmark {
public:
    S3ReadBenchmark(int iterations, const std::map<std::string, std::string>& conf_map)
            : BaseBenchmark("S3ReadBenchmark", iterations, conf_map), _result(buffer, 128) {}
    virtual ~S3ReadBenchmark() = default;

    Status init() override {
        bm_log("begin to init {}", _name);
        std::string file_path = _conf_map["file"];
        io::FileReaderOptions reader_opts = FileFactory::get_reader_options(nullptr);
        RETURN_IF_ERROR(
                FileFactory::create_s3_reader(_conf_map, file_path, &_fs, &_reader, reader_opts));
        bm_log("finish to init {}", _name);
        return Status::OK();
    }

    Status run() override { return _reader->read_at(0, _result, &_bytes_read); }

private:
    doris::S3Conf _s3_conf;
    std::shared_ptr<io::FileSystem> _fs;
    io::FileReaderSPtr _reader;
    char buffer[128];
    doris::Slice _result;
    size_t _bytes_read = 0;
};

} // namespace doris::io
