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

#include "common/status.h"
#include "olap/iterators.h"
#include "olap/schema.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"

#pragma once

namespace doris {

namespace vectorized {

// Row source represent row location in multi-segments
// use a uint16_t to store info
// the lower 15 bits means segment_id in segment pool, and the higher 1 bits means agg flag.
// In unique-key, agg flags means this key should be deleted, this comes from two way: old version
// key or delete_sign.
class RowSource {
public:
    RowSource(uint16_t data) : _data(data) {}
    RowSource(uint16_t source_num, bool agg_flag);

    uint16_t get_source_num();
    bool agg_flag();
    void set_agg_flag(bool agg_flag);
    uint16_t data() const;

private:
    uint16_t _data;
    static const uint16_t SOURCE_FLAG = 0x7FFF;
    static const uint16_t AGG_FLAG = 0x8000;
};

/* rows source buffer
this buffer should have a memory limit, once reach memory limit, write
buffer data to tmp file.
usage:
    RowSourcesBuffer buffer(tablet_id, tablet_storage_path, reader_type);
    buffer.append()
    buffer.append()
    buffer.flush()
    buffer.seek_to_begin()
    while (buffer.has_remaining().ok()) {
        auto cur = buffer.current().get_source_num();
        auto same = buffer.same_source_count(cur, limit);
        // do copy block data
        buffer.advance(same);
    }
*/
class RowSourcesBuffer {
public:
    RowSourcesBuffer(int64_t tablet_id, const std::string& tablet_path, ReaderType reader_type)
            : _tablet_id(tablet_id),
              _tablet_path(tablet_path),
              _reader_type(reader_type),
              _buffer(ColumnUInt16::create()) {}

    ~RowSourcesBuffer() {
        _reset_buffer();
        if (_fd > 0) {
            ::close(_fd);
        }
    }

    // write batch row source
    Status append(const std::vector<RowSource>& row_sources);
    Status flush();

    RowSource current() {
        DCHECK(_buf_idx < _buffer->size());
        return RowSource(_buffer->get_element(_buf_idx));
    }
    void advance(int32_t step = 1) {
        DCHECK(_buf_idx + step <= _buffer->size());
        _buf_idx += step;
    }

    uint64_t buf_idx() { return _buf_idx; }
    uint64_t total_size() { return _total_size; }
    uint64_t buffered_size() { return _buffer->size(); }
    void set_agg_flag(uint64_t index, bool agg);

    Status has_remaining();

    Status seek_to_begin();

    size_t same_source_count(uint16_t source, size_t limit);

private:
    Status _create_buffer_file();
    Status _serialize();
    Status _deserialize();
    void _reset_buffer() {
        _buffer->clear();
        _buf_idx = 0;
    }

private:
    int64_t _tablet_id;
    std::string _tablet_path;
    ReaderType _reader_type;
    uint64_t _buf_idx = 0;
    int _fd = -1;
    ColumnUInt16::MutablePtr _buffer;
    uint64_t _total_size = 0;
};

// --------------- VerticalMergeIteratorContext ------------- //
// takes ownership of rowwise iterator
class VerticalMergeIteratorContext {
public:
    VerticalMergeIteratorContext(RowwiseIterator* iter, size_t ori_return_cols, uint32_t order,
                                 uint32_t seq_col_idx)
            : _iter(iter),
              _ori_return_cols(ori_return_cols),
              _order(order),
              _seq_col_idx(seq_col_idx),
              _num_key_columns(iter->schema().num_key_columns()) {}

    VerticalMergeIteratorContext(const VerticalMergeIteratorContext&) = delete;
    VerticalMergeIteratorContext(VerticalMergeIteratorContext&&) = delete;
    VerticalMergeIteratorContext& operator=(const VerticalMergeIteratorContext&) = delete;
    VerticalMergeIteratorContext& operator=(VerticalMergeIteratorContext&&) = delete;

    ~VerticalMergeIteratorContext() {
        delete _iter;
        _iter = nullptr;
    }
    Status block_reset(const std::shared_ptr<Block>& block);
    Status init(const StorageReadOptions& opts);
    bool compare(const VerticalMergeIteratorContext& rhs) const;
    void copy_rows(Block* block, bool advanced = true);
    void copy_rows(Block* block, size_t count);

    Status advance();

    // Return if it has remaining data in this context.
    // Only when this function return true, current_row()
    // will return a valid row
    bool valid() const { return _valid; }

    uint32_t order() const { return _order; }

    void set_is_same(bool is_same) const { _is_same = is_same; }

    bool is_same() { return _is_same; }

    void add_cur_batch() { _cur_batch_num++; }

    bool is_cur_block_finished() { return _index_in_block == _block->rows() - 1; }

    size_t remain_rows() { return _block->rows() - _index_in_block; }

    bool is_first_row() { return _is_first_row; }
    void set_is_first_row(bool is_first_row) { _is_first_row = is_first_row; }
    void set_cur_row_ref(vectorized::IteratorRowRef* ref) {
        ref->block = _block;
        ref->row_pos = _index_in_block;
    }

private:
    // Load next block into _block
    Status _load_next_block();

    RowwiseIterator* _iter;
    size_t _ori_return_cols = 0;

    // segment order, used to compare key
    uint32_t _order = -1;

    uint32_t _seq_col_idx = -1;

    bool _valid = false;
    mutable bool _is_same = false;
    size_t _index_in_block = -1;
    size_t _block_row_max = 0;
    int _num_key_columns;
    size_t _cur_batch_num = 0;

    // used to store data load from iterator->next_batch(Block*)
    std::shared_ptr<Block> _block;
    // used to store data still on block view
    std::list<std::shared_ptr<Block>> _block_list;
    // use to identify whether it's first block load from RowwiseIterator
    bool _is_first_row = true;
};

// --------------- VerticalHeapMergeIterator ------------- //
class VerticalHeapMergeIterator : public RowwiseIterator {
public:
    // VerticalMergeIterator takes the ownership of input iterators
    VerticalHeapMergeIterator(std::vector<RowwiseIterator*> iters, size_t ori_return_cols,
                              KeysType keys_type, int32_t seq_col_idx,
                              RowSourcesBuffer* row_sources_buf)
            : _origin_iters(std::move(iters)),
              _ori_return_cols(ori_return_cols),
              _keys_type(keys_type),
              _seq_col_idx(seq_col_idx),
              _row_sources_buf(row_sources_buf) {}

    ~VerticalHeapMergeIterator() override {
        while (!_merge_heap.empty()) {
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            delete ctx;
        }
    }

    Status init(const StorageReadOptions& opts) override;
    Status next_batch(Block* block) override;
    const Schema& schema() const override { return *_schema; }
    uint64_t merged_rows() const override { return _merged_rows; }

private:
    int _get_size(Block* block) { return block->rows(); }

private:
    // It will be released after '_merge_heap' has been built.
    std::vector<RowwiseIterator*> _origin_iters;
    size_t _ori_return_cols;

    const Schema* _schema = nullptr;

    struct VerticalMergeContextComparator {
        bool operator()(const VerticalMergeIteratorContext* lhs,
                        const VerticalMergeIteratorContext* rhs) const {
            return lhs->compare(*rhs);
        }
    };

    using VMergeHeap = std::priority_queue<VerticalMergeIteratorContext*,
                                           std::vector<VerticalMergeIteratorContext*>,
                                           VerticalMergeContextComparator>;

    VMergeHeap _merge_heap;
    int _block_row_max = 0;
    KeysType _keys_type;
    int32_t _seq_col_idx = -1;
    RowSourcesBuffer* _row_sources_buf;
    uint32_t _merged_rows = 0;
};

// --------------- VerticalMaskMergeIterator ------------- //
class VerticalMaskMergeIterator : public RowwiseIterator {
public:
    // VerticalMaskMergeIterator takes the ownership of input iterators
    VerticalMaskMergeIterator(std::vector<RowwiseIterator*> iters, size_t ori_return_cols,
                              RowSourcesBuffer* row_sources_buf)
            : _origin_iters(std::move(iters)),
              _ori_return_cols(ori_return_cols),
              _row_sources_buf(row_sources_buf) {}

    ~VerticalMaskMergeIterator() override {
        for (auto iter : _origin_iter_ctx) {
            delete iter;
        }
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const Schema& schema() const override { return *_schema; }

    Status next_row(IteratorRowRef* ref) override;

    Status unique_key_next_row(IteratorRowRef* ref) override;

private:
    int _get_size(Block* block) { return block->rows(); }

private:
    // released after build ctx
    std::vector<RowwiseIterator*> _origin_iters;
    size_t _ori_return_cols = 0;

    std::vector<VerticalMergeIteratorContext*> _origin_iter_ctx;

    const Schema* _schema = nullptr;

    int _block_row_max = 0;
    RowSourcesBuffer* _row_sources_buf;
};

// segment merge iterator
std::shared_ptr<RowwiseIterator> new_vertical_heap_merge_iterator(
        const std::vector<RowwiseIterator*>& inputs, size_t _ori_return_cols, KeysType key_type,
        uint32_t seq_col_idx, RowSourcesBuffer* row_sources_buf);

std::shared_ptr<RowwiseIterator> new_vertical_mask_merge_iterator(
        const std::vector<RowwiseIterator*>& inputs, size_t ori_return_cols,
        RowSourcesBuffer* row_sources_buf);

} // namespace vectorized
} // namespace doris