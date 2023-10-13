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

#include "olap/delete_bitmap_calculator.h"

#include "common/status.h"
#include "olap/primary_key_index.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

Status MergeIndexDeleteBitmapCalculatorContext::get_current_key(Slice& slice) {
    if (_cur_row_id >= _num_rows) {
        return Status::EndOfFile("Reach the end of file");
    }
    if (_cur_pos >= _block_size) {
        RETURN_IF_ERROR(_iter->seek_to_ordinal(_cur_row_id));
        RETURN_IF_ERROR(_next_batch(_cur_row_id));
    }
    slice = Slice(_index_column->get_data_at(_cur_pos).data,
                  _index_column->get_data_at(_cur_pos).size);
    return Status::OK();
}

Status MergeIndexDeleteBitmapCalculatorContext::advance() {
    ++_cur_pos;
    ++_cur_row_id;
    if (_cur_row_id >= _num_rows) {
        return Status::EndOfFile("Reach the end of file");
    }
    return Status::OK();
}

Status MergeIndexDeleteBitmapCalculatorContext::seek_at_or_after(Slice const& key) {
    auto st = _iter->seek_at_or_after(&key, &_excat_match);
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        return Status::EndOfFile("Reach the end of file");
    }
    RETURN_IF_ERROR(st);
    auto current_ordinal = _iter->get_current_ordinal();
    DCHECK(current_ordinal > _cur_row_id)
            << fmt::format("current_ordinal: {} should be greater than _cur_row_id: {}",
                           current_ordinal, _cur_row_id);
    // if key is still in the block read before,
    // in other words, if `_cur_pos + current_ordinal - _cur_row_id < _block_size` holds
    // we can seek simply by moving the pointers, aka. _cur_pos and _cur_row_id
    if (_cur_pos + current_ordinal - _cur_row_id < _block_size) {
        _cur_pos = _cur_pos + current_ordinal - _cur_row_id;
        _cur_row_id = current_ordinal;
        return Status::OK();
    }
    // otherwise, we have to read the data starts from `current_ordinal`
    return _next_batch(current_ordinal);
}

Status MergeIndexDeleteBitmapCalculatorContext::_next_batch(size_t row_id) {
    // _iter should be seeked before calling this function
    DCHECK(row_id < _num_rows) << fmt::format("row_id: {} should be less than _num_rows: {}",
                                              row_id, _num_rows);
    _index_column = _index_type->create_column();
    auto remaining = _num_rows - row_id;
    size_t num_to_read = std::min(_max_batch_size, remaining);
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_iter->next_batch(&num_read, _index_column));
    DCHECK(num_to_read == num_read) << fmt::format(
            "num_to_read: {} should be equal to num_to_read: {}", num_to_read, num_read);
    _block_size = num_read;
    _cur_pos = 0;
    _cur_row_id = row_id;
    return Status::OK();
}

bool MergeIndexDeleteBitmapCalculatorContext::Comparator::operator()(
        MergeIndexDeleteBitmapCalculatorContext* lhs,
        MergeIndexDeleteBitmapCalculatorContext* rhs) const {
    // std::proiroty_queue is a max heap, and function should return the result of `lhs < rhs`
    // so if the result of the function is true, rhs will be popped before lhs
    Slice key1, key2;
    RETURN_IF_ERROR(lhs->get_current_key(key1));
    RETURN_IF_ERROR(rhs->get_current_key(key2));
    if (_sequence_length == 0) {
        auto cmp_result = key1.compare(key2);
        // when key1 is the same as key2,
        // we want the one with greater segment id to be popped first
        return cmp_result ? (cmp_result > 0) : (lhs->segment_id() < rhs->segment_id());
    }
    // smaller key popped first
    auto key1_without_seq = Slice(key1.get_data(), key1.get_size() - _sequence_length);
    auto key2_without_seq = Slice(key2.get_data(), key2.get_size() - _sequence_length);
    auto cmp_result = key1_without_seq.compare(key2_without_seq);
    if (cmp_result != 0) {
        return cmp_result > 0;
    }
    // greater sequence value popped first
    auto key1_sequence_val =
            Slice(key1.get_data() + key1.get_size() - _sequence_length, _sequence_length);
    auto key2_sequence_val =
            Slice(key2.get_data() + key2.get_size() - _sequence_length, _sequence_length);
    cmp_result = key1_sequence_val.compare(key2_sequence_val);
    if (cmp_result != 0) {
        return cmp_result < 0;
    }
    // greater segment id popped first
    return lhs->segment_id() < rhs->segment_id();
}

bool MergeIndexDeleteBitmapCalculatorContext::Comparator::is_key_same(Slice const& lhs,
                                                                      Slice const& rhs) const {
    DCHECK(lhs.get_size() >= _sequence_length);
    DCHECK(rhs.get_size() >= _sequence_length);
    auto lhs_without_seq = Slice(lhs.get_data(), lhs.get_size() - _sequence_length);
    auto rhs_without_seq = Slice(rhs.get_data(), rhs.get_size() - _sequence_length);
    return lhs_without_seq.compare(rhs_without_seq) == 0;
}

Status MergeIndexDeleteBitmapCalculator::init(RowsetId rowset_id,
                                              std::vector<SegmentSharedPtr> const& segments,
                                              size_t seq_col_length, size_t max_batch_size) {
    _rowset_id = rowset_id;
    _seq_col_length = seq_col_length;
    _comparator = MergeIndexDeleteBitmapCalculatorContext::Comparator(seq_col_length);
    _contexts.reserve(segments.size());
    _heap = std::make_unique<Heap>(_comparator);

    for (auto& segment : segments) {
        RETURN_IF_ERROR(segment->load_index());
        auto pk_idx = segment->get_primary_key_index();
        std::unique_ptr<segment_v2::IndexedColumnIterator> index;
        RETURN_IF_ERROR(pk_idx->new_iterator(&index));
        auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                pk_idx->type_info()->type(), 1, 0);
        _contexts.emplace_back(std::move(index), index_type, segment->id(), pk_idx->num_rows());
        _heap->push(&_contexts.back());
    }
    return Status::OK();
}

Status MergeIndexDeleteBitmapCalculator::calculate_one(RowLocation& loc) {
    // get the location of a out-of-date row
    while (!_heap->empty()) {
        auto cur_ctx = _heap->top();
        _heap->pop();
        Slice cur_key;
        RETURN_IF_ERROR(cur_ctx->get_current_key(cur_key));
        if (!_last_key.empty() && _comparator.is_key_same(cur_key, _last_key)) {
            loc.segment_id = cur_ctx->segment_id();
            loc.row_id = cur_ctx->row_id();
            auto st = cur_ctx->advance();
            if (st.ok()) {
                _heap->push(cur_ctx);
            } else if (!st.is<ErrorCode::END_OF_FILE>()) {
                return st;
            }
            return Status::OK();
        }
        if (_heap->empty()) {
            break;
        }
        _last_key = cur_key.to_string();
        auto nxt_ctx = _heap->top();
        Slice nxt_key;
        RETURN_IF_ERROR(nxt_ctx->get_current_key(nxt_key));
        Status st = _comparator.is_key_same(cur_key, nxt_key)
                            ? cur_ctx->advance()
                            : cur_ctx->seek_at_or_after(Slice(
                                      nxt_key.get_data(), nxt_key.get_size() - _seq_col_length));
        if (st.is<ErrorCode::END_OF_FILE>()) {
            continue;
        }
        RETURN_IF_ERROR(st);
        _heap->push(cur_ctx);
    }
    return Status::EndOfFile("Reach end of file");
}

Status MergeIndexDeleteBitmapCalculator::calculate_all(DeleteBitmapPtr delete_bitmap) {
    RowLocation loc;
    while (true) {
        auto st = calculate_one(loc);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        RETURN_IF_ERROR(st);
        delete_bitmap->add({_rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                           loc.row_id);
    }
    return Status::OK();
}

} // namespace doris
