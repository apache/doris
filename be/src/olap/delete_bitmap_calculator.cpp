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

#include <gen_cpp/types.pb.h>

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
    lhs->get_current_key(&key1);
    rhs->get_current_key(&key2);
    // We start by comparing the key portions of the two records.
    // If they are not equal, we want the record with the smaller key to be popped from the heap first.
    // Therefore, we need to return the result of key1 > key2.
    int key_cmp_result = compare_key(key1, key2);
    if (LIKELY(key_cmp_result != 0)) {
        return key_cmp_result > 0;
    }
    // If key portions of the two records are equal,
    // we want the record with the larger sequence value to be popped from the heap first.
    // Therefore, we need to return the result of seq1 < seq2.
    int seq_cmp_result = compare_seq(key1, key2);
    if (LIKELY(seq_cmp_result != 0)) {
        return seq_cmp_result < 0;
    }
    // Otherwise, if they have the same key and sequence value,
    // we want the record with the larger version value to be popped from the heap first.
    // Therefore, we need to return the result of version1 < version2.
    if (LIKELY(lhs->end_version() != rhs->end_version())) {
        return lhs->end_version() < rhs->end_version();
    }
    // Lastly, if they have the same key, sequence value and version,
    // we want the record with the larger segment id to be popped from the heap first.
    // Therefore, we need to return the result of segid1 < segid2.
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
    if (LIKELY(_sequence_length == 0)) {
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
                                                 size_t num_delta_segments, size_t seq_col_length,
                                                 size_t max_batch_size) {
    // The first `num_delta_segments` segments are the delta data, others are the base data.
    DCHECK_LE(num_delta_segments, segments.size());
    _seq_col_length = seq_col_length;
    _comparator = MergedPKIndexDeleteBitmapCalculatorContext::Comparator(seq_col_length);
    size_t num_segments = segments.size();
    _contexts.reserve(num_segments);
    _heap = std::make_unique<Heap>(_comparator);

    for (size_t i = 0; i < num_segments; ++i) {
        auto&& segment = segments[i];
        int64_t end_version = end_versions ? (*end_versions)[i] : 0;
        RETURN_IF_ERROR(segment->load_index());
        auto pk_idx = segment->get_primary_key_index();
        std::unique_ptr<segment_v2::IndexedColumnIterator> index;
        RETURN_IF_ERROR(pk_idx->new_iterator(&index));
        auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                pk_idx->type_info()->type(), 1, 0);
        bool is_delta_segment = i < num_delta_segments;
        _contexts.emplace_back(std::move(index), index_type, segment->rowset_id(), end_version,
                               segment->id(), is_delta_segment, pk_idx->num_rows(), max_batch_size);
        _heap->push(&_contexts.back());
    }
    return Status::OK();
}

Status MergedPKIndexDeleteBitmapCalculator::init(
        std::vector<SegmentSharedPtr> const& segments,
        const std::vector<RowsetSharedPtr>* specified_rowsets, size_t seq_col_length,
        size_t max_batch_size) {
    std::vector<SegmentSharedPtr> all_segments(segments);
    // Assume that the delta segments' version is infinitely large.
    std::vector<int64_t> end_versions(segments.size(), INT64_MAX);
    _calc_delete_bitmap_between_rowsets = specified_rowsets != nullptr;
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
    return init(all_segments, &end_versions, segments.size(), seq_col_length, max_batch_size);
}

Status MergedPKIndexDeleteBitmapCalculator::process(DeleteBitmapPtr delete_bitmap) {
    while (_heap->size() >= 2) {
        if (_calc_delete_bitmap_between_rowsets) {
            // This is a trick when we don't need to calculating the delete bitmap between delta data.
            // Assuming that some smallest contexts correspond to segments of the base data,
            // we only need to directly seek these contexts to the key represented
            // by the smallest context of the delta data.
            // It is the same if the smallest contexts correspond to segments of the delta data.
            bool should_return = false;
            while (true) {
                auto st = step();
                if (st.ok()) {
                    break;
                }
                if (st.is<ErrorCode::NOT_FOUND>()) {
                    should_return = true;
                    break;
                }
                if (st.is<ErrorCode::END_OF_FILE>()) {
                    continue;
                }
                RETURN_IF_ERROR(st);
            }
            if (should_return) {
                break;
            }
        }
        // We pop two context ctx1 and ctx2 from the heap.
        // Apprently, ctx1's key is smaller than or equal to ctx2' key.
        auto ctx1 = _heap->top();
        _heap->pop();
        auto ctx2 = _heap->top();
        _heap->pop();

        Slice key1, key2;
        RETURN_IF_ERROR(ctx1->get_current_key(&key1));
        RETURN_IF_ERROR(ctx2->get_current_key(&key2));

        if (!_comparator.is_key_same(key1, key2)) {
            // If key1 is not equal to key2, we can say that key1 is smaller than key2.
            // So, we seek ctx1 to ctx2.
            auto key2_without_seq = Slice(key2.get_data(), key2.get_size() - _seq_col_length);
            auto st = ctx1->seek_at_or_after(key2_without_seq);
            if (UNLIKELY(!(st.ok() || st.is<ErrorCode::END_OF_FILE>()))) {
                return st;
            }
        } else {
            // If key1 is equal to key2, the row ctx2 pointed now is to be deleted.
            delete_bitmap->add({ctx2->rowset_id(), ctx2->segment_id(), 0}, ctx2->row_id());
            ctx2->advance();
        }

        if (!ctx1->eof()) {
            _heap->push(ctx1);
        }
        if (!ctx2->eof()) {
            _heap->push(ctx2);
        }
    }
    return Status::OK();
}

Status MergedPKIndexDeleteBitmapCalculator::step() {
    // `ctxs_to_seek` are the smallest contexts of same type of data (either base data or delta data).
    // `target_ctx` is the context next to `ctxs_to_seek`
    std::vector<MergedPKIndexDeleteBitmapCalculatorContext*> ctxs_to_seek;
    MergedPKIndexDeleteBitmapCalculatorContext* target_ctx = nullptr;
    while (!_heap->empty()) {
        auto ctx = _heap->top();
        if (!ctxs_to_seek.empty() &&
            ctx->is_delta_segment() != ctxs_to_seek.back()->is_delta_segment()) {
            target_ctx = ctx;
            break;
        }
        _heap->pop();
        ctxs_to_seek.emplace_back(ctx);
    }
    // If `target_ctx` is nullptr, we can say that all of the contexts in heap are the same type of data.
    // So there is no duplicate keys, and we should end this algorithm by returning not found error.
    if (target_ctx == nullptr) {
        return Status::NotFound("No target found");
    }
    // Otherwise, we then seek all the contexts in `ctxs_to_seek` to `target_ctx`
    // and append them to `ctxs_to_push`.
    Slice key;
    RETURN_IF_ERROR(target_ctx->get_current_key(&key));
    key = Slice(key.get_data(), key.get_size() - _seq_col_length);
    std::vector<MergedPKIndexDeleteBitmapCalculatorContext*> ctxs_to_push;
    for (auto ctx : ctxs_to_seek) {
        auto st = ctx->seek_at_or_after(key);
        if (UNLIKELY(!(st.ok() || st.is<ErrorCode::END_OF_FILE>()))) {
            return st;
        }
        if (!ctx->eof()) {
            ctxs_to_push.emplace_back(ctx);
        }
    }
    // If all of the contexts in `ctxs_to_seek` reach the eof,
    // we can go for another `step` process by returning end of file error.
    if (ctxs_to_push.empty()) {
        return Status::EndOfFile("No ctx to push");
    }
    // Since contexts in `ctxs_to_push` lose their order,
    // we have to sort them for further seek.
    std::sort(ctxs_to_push.begin(), ctxs_to_push.end(), _comparator);
    std::reverse(ctxs_to_push.begin(), ctxs_to_push.end());
    // If some smallest contexts of `ctxs_to_push` have the same key,
    // we have to advance the contexts whoses record pointed now are deleted.
    Slice first_key;
    for (size_t i = 0; i < ctxs_to_push.size(); ++i) {
        auto ctx = ctxs_to_push[i];
        if (i == 0) {
            RETURN_IF_ERROR(ctx->get_current_key(&first_key));
        } else {
            Slice cur_key;
            RETURN_IF_ERROR(ctx->get_current_key(&cur_key));
            if (_comparator.is_key_same(cur_key, first_key)) {
                ctx->advance();
            }
        }
        if (!ctx->eof()) {
            _heap->push(ctx);
        }
    }
    return Status::OK();
}

} // namespace doris
