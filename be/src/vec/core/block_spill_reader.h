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

#include "io/fs/file_reader.h"
#include "vec/core/block.h"

namespace doris {
namespace vectorized {
// Read data spilled to local file.
class BlockSpillReader {
public:
    BlockSpillReader(const std::string& file_path) : file_path_(file_path) {}

    Status open();

    Status close() {
        file_reader_.reset();
        return Status::OK();
    }

    // read a small block, set eof to true if no more data to read
    Status read(Block& block, bool& eof);

private:
    std::string file_path_;
    io::FileReaderSPtr file_reader_;

    size_t block_count_ = 0;
    size_t read_block_index_ = 0;
    std::vector<size_t> block_start_offsets_;
};

using BlockSpillReaderUPtr = std::unique_ptr<BlockSpillReader>;
} // namespace vectorized
} // namespace doris
