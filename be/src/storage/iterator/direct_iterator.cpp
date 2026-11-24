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

#include "storage/iterator/direct_iterator.h"

#include <utility>

#include "core/block/block.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"

namespace doris {

DirectIterator::DirectIterator(std::vector<RowsetReaderSharedPtr> rs_readers,
                               RowsetReaderContext* read_context, OlapReaderStatistics* stats)
        : _rs_readers(std::move(rs_readers)), _read_context(read_context), _stats(stats) {}

Status DirectIterator::_advance_segment(bool* has_segment) {
    while (true) {
        if (_next_segment < _segments.size()) {
            _cur_segment = std::move(_segments[_next_segment++]);
            *has_segment = true;
            return Status::OK();
        }
        _segments.clear();
        _next_segment = 0;
        _cur_rs_reader.reset();
        if (_next_rowset >= _rs_readers.size()) {
            *has_segment = false;
            return Status::OK();
        }
        _cur_rs_reader = std::move(_rs_readers[_next_rowset++]);
        RETURN_IF_ERROR(_cur_rs_reader->get_segment_iterators(_read_context, &_segments));
    }
}

Status DirectIterator::next_block(Block* block, bool* eof) {
    *eof = false;
    RuntimeState* runtime_state = _read_context->runtime_state;
    while (true) {
        if (_cur_segment == nullptr) {
            bool has_segment = false;
            RETURN_IF_ERROR(_advance_segment(&has_segment));
            if (!has_segment) {
                *eof = true;
                return Status::OK();
            }
        }
        Status st;
        {
            SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
            block->clear_column_data();
            st = _cur_segment->next_batch(block);
        }
        if (st.is<ErrorCode::END_OF_FILE>()) {
            _cur_segment.reset();
            continue;
        }
        RETURN_IF_ERROR(st);
        if (runtime_state != nullptr && runtime_state->is_cancelled()) [[unlikely]] {
            return runtime_state->cancel_reason();
        }
        if (block->rows() > 0) {
            return Status::OK();
        }
    }
}

void DirectIterator::update_profile(RuntimeProfile* profile) {
    if (_cur_segment != nullptr) {
        _cur_segment->update_profile(profile);
    }
}

} // namespace doris
