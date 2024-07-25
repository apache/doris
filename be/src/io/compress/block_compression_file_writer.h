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

#include "common/status.h"
#include "io/compress/stream_compression_file_writer.h"

namespace doris::io {

class BlockCompressionFileWriter : public StreamCompressionFileWriter {
public:
    BlockCompressionFileWriter(std::unique_ptr<FileWriter> file_writer,
                               std::unique_ptr<Compressor> compressor, size_t buffer_size,
                               size_t compression_overhead)
            : StreamCompressionFileWriter(std::move(file_writer), std::move(compressor),
                                          buffer_size),
              _max_input_size(buffer_size - compression_overhead) {}

    Status appendv(const Slice* data, size_t data_cnt) override;
    Status finish() override;

protected:
    Status _compress() override;

private:
    Status _write_length(size_t length);
    const size_t _max_input_size;
};

} // namespace doris::io