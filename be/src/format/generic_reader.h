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

#include <cstddef>

#include "common/status.h"
#include "util/profile_collector.h"

namespace doris {

class Block;

// Shared block interface for metadata scans and the JDBC scanner. File scans use format::TableReader.
class GenericReader : public ProfileCollector {
public:
    ~GenericReader() override = default;

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) {
        return _do_get_next_block(block, read_rows, eof);
    }
    virtual Status close() { return Status::OK(); }
    virtual void set_batch_size(size_t) {}
    virtual size_t get_batch_size() const { return 0; }

protected:
    virtual Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;
};

} // namespace doris
