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

#include "storage/iterator/agg_key_block_iterator.h"

#include <utility>

#include "common/exception.h"
#include "storage/iterators.h"

namespace doris {

AggKeyBlockIterator::AggKeyBlockIterator(RowwiseIteratorUPtr input,
                                         std::vector<uint32_t> key_ordinals, size_t max_rows,
                                         size_t preferred_block_size_bytes,
                                         ReadSchemaSPtr read_schema, int be_exec_version,
                                         uint64_t* merged_rows)
        : _input(std::move(input)),
          _read_schema(read_schema),
          _be_exec_version(be_exec_version),
          _aggregator(std::move(read_schema), std::move(key_ordinals), max_rows,
                      preferred_block_size_bytes, merged_rows) {}

Status AggKeyBlockIterator::init() {
    _input_block = _read_schema->create_read_block();
    return _aggregator.init(_be_exec_version);
}

Status AggKeyBlockIterator::next_block(Block* block, bool* eof) {
    *eof = false;
    while (!_input_done) {
        if (_aggregator.source_exhausted()) {
            _input_block.clear_column_data();
            Status st = _input->next_batch(&_input_block);
            if (st.is<ErrorCode::END_OF_FILE>()) {
                _input_done = true;
                break;
            }
            RETURN_IF_ERROR(st);
            _aggregator.update_source(_input_block);
        }
        RETURN_IF_CATCH_EXCEPTION(_aggregator.aggregate());
        // The batch is full in front of a new key.
        if (!_aggregator.source_exhausted() && _aggregator.is_finish()) {
            RETURN_IF_CATCH_EXCEPTION(_aggregator.aggregate_result(block));
            return Status::OK();
        }
    }
    if (_aggregator.has_aggregate_data()) {
        RETURN_IF_CATCH_EXCEPTION(_aggregator.aggregate_result(block));
        return Status::OK();
    }
    *eof = true;
    return Status::OK();
}

} // namespace doris
