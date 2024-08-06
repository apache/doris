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

#include "schema_dummy_scanner.h"

#include <vector>

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace {
std::vector<doris::SchemaScanner::ColumnDesc> DUMMY_COLUMN;
}

namespace doris {

SchemaDummyScanner::SchemaDummyScanner() : SchemaScanner(DUMMY_COLUMN) {}

SchemaDummyScanner::~SchemaDummyScanner() {}

Status SchemaDummyScanner::start(RuntimeState* state) {
    return Status::OK();
}

Status SchemaDummyScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    *eos = true;
    return Status::OK();
}

} // namespace doris
