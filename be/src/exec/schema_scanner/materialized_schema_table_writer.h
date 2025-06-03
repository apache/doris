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
#include <string>

#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace vectorized {
class Block;
} // namespace vectorized

class MaterializedSchemaTableDir;
class MaterializedSchemaTableWriter {
public:
    MaterializedSchemaTableWriter(MaterializedSchemaTableDir* dir) : dir_(dir) {}

    Status write(const vectorized::Block& block, size_t& written_bytes, const std::string& file_path);

    size_t get_written_blocks() const { return written_blocks_; }

    int64_t get_written_bytes() const { return total_written_bytes_; }

private:
    // for checking disk capacity when write data to disk.
    MaterializedSchemaTableDir* dir_ = nullptr;
    size_t written_blocks_ = 0;
    int64_t total_written_bytes_ = 0;
};
using MaterializedSchemaTableWriterUPtr = std::unique_ptr<MaterializedSchemaTableWriter>;
} // namespace doris

#include "common/compile_check_end.h"
