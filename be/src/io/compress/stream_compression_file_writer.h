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

#include <memory>

#include "common/status.h"
#include "io/compress/compression_file_writer.h"

namespace doris::io {

class StreamCompressionFileWriter : public CompressionFileWriter {
public:
    StreamCompressionFileWriter(std::unique_ptr<FileWriter> file_writer,
                                std::unique_ptr<Compressor> compressor, size_t buffer_size)
            : CompressionFileWriter(std::move(file_writer), std::move(compressor)),
              _buffer_size(buffer_size) {}
    ~StreamCompressionFileWriter() override {
        if (_buffer != nullptr) {
            delete[] _buffer;
            _buffer = nullptr;
        }
    };

    Status appendv(const Slice* data, size_t data_cnt) override;
    Status init() override;
    Status finish() override;

protected:
    virtual Status _compress();
    char* _buffer = nullptr;
    size_t _buffer_size = 0;
};

} // namespace doris::io