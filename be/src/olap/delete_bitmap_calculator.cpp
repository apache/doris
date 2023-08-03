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

#include <iterator>

#include "common/status.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/segment_loader.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

Status MergedPKIndexDeleteBitmapCalculatorContext::get_current_key(Slice* slice) {
    if (_cur_row_id >= _num_rows) {
        return Status::EndOfFile("Reach the end of file");
    }
    if (_cur_pos >= _block_size) {
        RETURN_IF_ERROR(_iter->seek_to_ordinal(_cur_row_id));
        RETURN_IF_ERROR(_next_batch(_cur_row_id));
    }
    *slice = Slice(_index_column->get_data_at(_cur_pos).data,
                   _index_column->get_data_at(_cur_pos).size);
    return Status::OK();
}

Status MergedPKIndexDeleteBitmapCalculatorContext::advance() {
    ++_cur_pos;
    ++_cur_row_id;
    if (_cur_row_id >= _num_rows) {
        return Status::EndOfFile("Reach the end of file");
    }
    return Status::OK();
}

Status MergedPKIndexDeleteBitmapCalculatorContext::seek_at_or_after(Slice const& key) {
    auto st = _iter->seek_at_or_after(&key, &_excat_match);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        _cur_row_id = _num_rows;
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

Status MergedPKIndexDeleteBitmapCalculatorContext::_next_batch(size_t row_id) {
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

bool MergedPKIndexDeleteBitmapCalculatorContext::Comparator::operator()(
        MergedPKIndexDeleteBitmapCalculatorContext* lhs,
        MergedPKIndexDeleteBitmapCalculatorContext* rhs) const {
    // std::proiroty_queue is a max heap, and function should return the result of `lhs < rhs`
    // so if the result of the function is true, rhs will be popped before lhs
    Slice key1, key2;
    Status st;
    st = lhs->get_current_key(&key1);
    CHECK(LIKELY(st.ok())) << fmt::format("Reading key1 but get st = {}", st);
    st = rhs->get_current_key(&key2);
    CHECK(LIKELY(st.ok())) << fmt::format("Reading key2 but get st = {}", st);
    int key_cmp_result = compare_key(key1, key2);
    if (LIKELY(key_cmp_result != 0)) {
        return key_cmp_result > 0;
    }
    int seq_cmp_result = compare_seq(key1, key2);
    if (LIKELY(seq_cmp_result != 0)) {
        return seq_cmp_result < 0;
    }
    if (LIKELY(lhs->end_version() != rhs->end_version())) {
        return lhs->end_version() < rhs->end_version();
    }
    return lhs->segment_id() < rhs->segment_id();
}

int MergedPKIndexDeleteBitmapCalculatorContext::Comparator::compare_key(Slice const& lhs,
                                                                        Slice const& rhs) const {
    auto lhs_without_seq = Slice(lhs.get_data(), lhs.get_size() - _sequence_length);
    auto rhs_without_seq = Slice(rhs.get_data(), rhs.get_size() - _sequence_length);
    return lhs_without_seq.compare(rhs_without_seq);
}

int MergedPKIndexDeleteBitmapCalculatorContext::Comparator::compare_seq(Slice const& lhs,
                                                                        Slice const& rhs) const {
    if (UNLIKELY(_sequence_length == 0)) {
        return 0;
    }
    auto lhs_seq = Slice(lhs.get_data() + lhs.get_size() - _sequence_length, _sequence_length);
    auto rhs_seq = Slice(rhs.get_data() + rhs.get_size() - _sequence_length, _sequence_length);
    return lhs_seq.compare(rhs_seq);
}

bool MergedPKIndexDeleteBitmapCalculatorContext::Comparator::is_key_same(Slice const& lhs,
                                                                         Slice const& rhs) const {
    return compare_key(lhs, rhs) == 0;
}

Status MergedPKIndexDeleteBitmapCalculator::init(std::vector<SegmentSharedPtr> const& segments,
                                                 const std::vector<int64_t>* end_versions,
                                                 size_t seq_col_length, size_t max_batch_size) {
    _seq_col_length = seq_col_length;
    _comparator = MergedPKIndexDeleteBitmapCalculatorContext::Comparator(seq_col_length);
    size_t num_segments = segments.size();
    _contexts.reserve(num_segments);
    _heap = std::make_unique<Heap>(_comparator);

    for (size_t i = 0; i < num_segments; ++i) {
        auto&& segment = segments[i];
        int64_t end_version = end_versions ? (*end_versions)[i] : 0;
        segment->rowset_id();
        RETURN_IF_ERROR(segment->load_index());
        auto pk_idx = segment->get_primary_key_index();
        std::unique_ptr<segment_v2::IndexedColumnIterator> index;
        RETURN_IF_ERROR(pk_idx->new_iterator(&index));
        auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                pk_idx->type_info()->type(), 1, 0);
        _contexts.emplace_back(std::move(index), index_type, segment->rowset_id(), end_version,
                               segment->id(), pk_idx->num_rows());
        _heap->push(&_contexts.back());
    }
    return Status::OK();
}

Status MergedPKIndexDeleteBitmapCalculator::init(
        std::vector<SegmentSharedPtr> const& segments,
        const std::vector<RowsetSharedPtr>* specified_rowsets, size_t seq_col_length,
        size_t max_batch_size) {
    std::vector<SegmentSharedPtr> all_segments(segments);
    std::vector<int64_t> end_versions(segments.size(), INT64_MAX);
    if (specified_rowsets) {
        size_t num_rowsets = specified_rowsets->size();
        _seg_caches = std::make_unique<std::vector<std::unique_ptr<SegmentCacheHandle>>>(
                specified_rowsets->size());
        for (size_t i = 0; i < num_rowsets; ++i) {
            auto&& rs = (*specified_rowsets)[i];
            auto&& end_version = rs->version().second;
            (*_seg_caches)[i] = std::make_unique<SegmentCacheHandle>();
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                    std::static_pointer_cast<BetaRowset>(rs), (*_seg_caches)[i].get(), true));
            auto&& segments = (*_seg_caches)[i]->get_segments();
            for (auto&& segment : segments) {
                all_segments.emplace_back(segment);
                end_versions.emplace_back(end_version);
            }
        }
    }
    return init(all_segments, &end_versions, seq_col_length, max_batch_size);
}

Status MergedPKIndexDeleteBitmapCalculator::process(DeleteBitmapPtr delete_bitmap) {
    while (_heap->size() >= 2) {
        auto iter1 = _heap->top();
        _heap->pop();
        auto iter2 = _heap->top();
        _heap->pop();

        Slice key1, key2;
        RETURN_IF_ERROR(iter1->get_current_key(&key1));
        RETURN_IF_ERROR(iter2->get_current_key(&key2));

        if (LIKELY(!_comparator.is_key_same(key1, key2))) {
            auto key2_without_seq = Slice(key2.get_data(), key2.get_size() - _seq_col_length);
            auto st = iter1->seek_at_or_after(key2_without_seq);
            if (UNLIKELY(!(st.ok() || st.is<ErrorCode::END_OF_FILE>() ||
                           st.is<ErrorCode::NOT_FOUND>()))) {
                return st;
            }
        } else {
            delete_bitmap->add({iter2->rowset_id(), iter2->segment_id(), 0}, iter2->row_id());
            iter2->advance();
        }

        if (!iter1->eof()) {
            _heap->push(iter1);
        }
        if (!iter2->eof()) {
            _heap->push(iter2);
        }
    }
    return Status::OK();
}

} // namespace doris
