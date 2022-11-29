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

#include "vec/olap/vertical_merge_iterator.h"

namespace doris {

namespace vectorized {

// --------------  row source  ---------------//
RowSource::RowSource(uint16_t source_num, bool agg_flag) {
    _data = (source_num & SOURCE_FLAG) | (source_num & AGG_FLAG);
    _data = agg_flag ? (_data | AGG_FLAG) : (_data & SOURCE_FLAG);
}

uint16_t RowSource::get_source_num() {
    return _data & SOURCE_FLAG;
}

bool RowSource::agg_flag() {
    return (_data & AGG_FLAG) != 0;
}

void RowSource::set_agg_flag(bool agg_flag) {
    _data = agg_flag ? (_data | AGG_FLAG) : (_data & SOURCE_FLAG);
}

uint16_t RowSource::data() const {
    return _data;
}

/*  --------------  row source buffer -------------   */

// current row_sources must save in memory so agg key can update agg flag
Status RowSourcesBuffer::append(const std::vector<RowSource>& row_sources) {
    if (_buffer->allocated_bytes() + row_sources.size() * sizeof(UInt16) >
        config::vertical_compaction_max_row_source_memory_mb * 1024 * 1024) {
        // serialize current buffer
        RETURN_IF_ERROR(_create_buffer_file());
        RETURN_IF_ERROR(_serialize());
        _reset_buffer();
    }
    for (const auto& source : row_sources) {
        _buffer->insert_value(source.data());
    }
    _total_size += row_sources.size();
    return Status::OK();
}

Status RowSourcesBuffer::seek_to_begin() {
    _buf_idx = 0;
    if (_fd > 0) {
        auto offset = lseek(_fd, 0, SEEK_SET);
        if (offset != 0) {
            LOG(WARNING) << "failed to seek to 0";
            return Status::InternalError("failed to seek to 0");
        }
        _reset_buffer();
    }
    return Status::OK();
}

Status RowSourcesBuffer::has_remaining() {
    if (_buf_idx < _buffer->size()) {
        return Status::OK();
    }
    DCHECK(_buf_idx == _buffer->size());
    if (_fd > 0) {
        _reset_buffer();
        auto st = _deserialize();
        if (!st.ok()) {
            return st;
        }
        return Status::OK();
    }
    return Status::EndOfFile("end of row source buffer");
}

void RowSourcesBuffer::set_agg_flag(uint64_t index, bool agg) {
    DCHECK(index < _buffer->size());
    RowSource ori(_buffer->get_data()[index]);
    ori.set_agg_flag(agg);
    _buffer->get_data()[index] = ori.data();
}

size_t RowSourcesBuffer::same_source_count(uint16_t source, size_t limit) {
    int result = 1;
    int start = _buf_idx + 1;
    int end = _buffer->size();
    while (result < limit && start < end) {
        RowSource next(_buffer->get_element(start++));
        if (source != next.get_source_num()) {
            break;
        }
        ++result;
    }
    return result;
}

Status RowSourcesBuffer::_create_buffer_file() {
    if (_fd != -1) {
        return Status::OK();
    }
    std::stringstream file_path_ss;
    file_path_ss << _tablet_path << "/compaction_row_source_" << _tablet_id;
    if (_reader_type == READER_BASE_COMPACTION) {
        file_path_ss << "_base";
    } else if (_reader_type == READER_CUMULATIVE_COMPACTION) {
        file_path_ss << "_cumu";
    } else {
        DCHECK(false);
        return Status::InternalError("unknown reader type");
    }
    file_path_ss << ".XXXXXX";
    std::string file_path = file_path_ss.str();
    LOG(INFO) << "Vertical compaction row sources buffer path: " << file_path;
    _fd = mkstemp(file_path.data());
    if (_fd < 0) {
        LOG(WARNING) << "failed to create tmp file, file_path=" << file_path;
        return Status::InternalError("failed to create tmp file");
    }
    // file will be released after fd is close
    unlink(file_path.data());
    return Status::OK();
}

Status RowSourcesBuffer::flush() {
    if (_fd > 0 && !_buffer->empty()) {
        RETURN_IF_ERROR(_serialize());
        _reset_buffer();
    }
    return Status::OK();
}

Status RowSourcesBuffer::_serialize() {
    size_t rows = _buffer->size();
    if (rows == 0) {
        return Status::OK();
    }
    // write size
    ssize_t bytes_written = ::write(_fd, &rows, sizeof(rows));
    if (bytes_written != sizeof(size_t)) {
        LOG(WARNING) << "failed to write buffer size to file, bytes_written=" << bytes_written;
        return Status::InternalError("fail to write buffer size to file");
    }
    // write data
    StringRef ref = _buffer->get_raw_data();
    bytes_written = ::write(_fd, ref.data, ref.size * sizeof(UInt16));
    if (bytes_written != _buffer->byte_size()) {
        LOG(WARNING) << "failed to write buffer data to file, bytes_written=" << bytes_written
                     << " buffer size=" << _buffer->byte_size();
        return Status::InternalError("fail to write buffer size to file");
    }
    return Status::OK();
}

Status RowSourcesBuffer::_deserialize() {
    size_t rows = 0;
    ssize_t bytes_read = ::read(_fd, &rows, sizeof(rows));
    if (bytes_read == 0) {
        LOG(WARNING) << "end of row source buffer file";
        return Status::EndOfFile("end of row source buffer file");
    } else if (bytes_read != sizeof(size_t)) {
        LOG(WARNING) << "failed to read buffer size from file, bytes_read=" << bytes_read;
        return Status::InternalError("failed to read buffer size from file");
    }
    _buffer->resize(rows);
    auto& internal_data = _buffer->get_data();
    bytes_read = ::read(_fd, internal_data.data(), rows * sizeof(UInt16));
    if (bytes_read != rows * sizeof(UInt16)) {
        LOG(WARNING) << "failed to read buffer data from file, bytes_read=" << bytes_read
                     << ", expect bytes=" << rows * sizeof(UInt16);
        return Status::InternalError("failed to read buffer data from file");
    }
    return Status::OK();
}

// ----------  vertical merge iterator context ----------//
Status VerticalMergeIteratorContext::block_reset(const std::shared_ptr<Block>& block) {
    if (!*block) {
        const Schema& schema = _iter->schema();
        const auto& column_ids = schema.column_ids();
        for (size_t i = 0; i < schema.num_column_ids(); ++i) {
            auto column_desc = schema.column(column_ids[i]);
            auto data_type = Schema::get_data_type_ptr(*column_desc);
            if (data_type == nullptr) {
                return Status::RuntimeError("invalid data type");
            }
            auto column = data_type->create_column();
            column->reserve(_block_row_max);
            block->insert(ColumnWithTypeAndName(std::move(column), data_type, column_desc->name()));
        }
    } else {
        block->clear_column_data();
    }
    return Status::OK();
}

bool VerticalMergeIteratorContext::compare(const VerticalMergeIteratorContext& rhs) const {
    int cmp_res = _block->compare_at(_index_in_block, rhs._index_in_block, _num_key_columns,
                                     *rhs._block, -1);
    if (cmp_res != 0) {
        return cmp_res > 0;
    }
    auto col_cmp_res = 0;
    if (_seq_col_idx != -1) {
        DCHECK(_block->columns() >= _num_key_columns);
        auto real_seq_idx = _num_key_columns;
        col_cmp_res = _block->compare_column_at(_index_in_block, rhs._index_in_block, real_seq_idx,
                                                *rhs._block, -1);
    }
    auto result = (col_cmp_res == 0) ? (_order < rhs.order()) : (col_cmp_res < 0);
    result ? set_is_same(true) : rhs.set_is_same(true);
    return result;
}

void VerticalMergeIteratorContext::copy_rows(Block* block, size_t count) {
    Block& src = *_block;
    Block& dst = *block;
    DCHECK(count > 0);

    auto start = _index_in_block;
    _index_in_block += count - 1;

    for (size_t i = 0; i < _ori_return_cols; ++i) {
        auto& s_col = src.get_by_position(i);
        auto& d_col = dst.get_by_position(i);

        ColumnPtr& s_cp = s_col.column;
        ColumnPtr& d_cp = d_col.column;

        d_cp->assume_mutable()->insert_range_from(*s_cp, start, count);
    }
}
// `advanced = false` when current block finished
void VerticalMergeIteratorContext::copy_rows(Block* block, bool advanced) {
    Block& src = *_block;
    Block& dst = *block;
    if (_cur_batch_num == 0) {
        return;
    }

    // copy a row to dst block column by column
    size_t start = _index_in_block - _cur_batch_num + 1 - advanced;
    DCHECK(start >= 0);

    for (size_t i = 0; i < _ori_return_cols; ++i) {
        auto& s_col = src.get_by_position(i);
        auto& d_col = dst.get_by_position(i);

        ColumnPtr& s_cp = s_col.column;
        ColumnPtr& d_cp = d_col.column;

        d_cp->assume_mutable()->insert_range_from(*s_cp, start, _cur_batch_num);
    }
    _cur_batch_num = 0;
}

Status VerticalMergeIteratorContext::init(const StorageReadOptions& opts) {
    _block_row_max = opts.block_row_max;
    RETURN_IF_ERROR(_load_next_block());
    if (valid()) {
        RETURN_IF_ERROR(advance());
    }
    return Status::OK();
}

Status VerticalMergeIteratorContext::advance() {
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (LIKELY(_index_in_block < _block->rows())) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}

Status VerticalMergeIteratorContext::_load_next_block() {
    do {
        if (_block != nullptr) {
            _block_list.push_back(_block);
            _block = nullptr;
        }
        for (auto it = _block_list.begin(); it != _block_list.end(); it++) {
            if (it->use_count() == 1) {
                block_reset(*it);
                _block = *it;
                _block_list.erase(it);
                break;
            }
        }
        if (_block == nullptr) {
            _block = std::make_shared<Block>();
            block_reset(_block);
        }
        Status st = _iter->next_batch(_block.get());
        if (!st.ok()) {
            _valid = false;
            if (st.is_end_of_file()) {
                return Status::OK();
            } else {
                return st;
            }
        }
        // erase delete handler columns
        if (_block->columns() > _ori_return_cols) {
            for (auto i = _block->columns() - 1; i >= _ori_return_cols; --i) {
                _block->erase(i);
            }
        }
    } while (_block->rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

//  ----------------  VerticalHeapMergeIterator  -------------  //
Status VerticalHeapMergeIterator::next_batch(Block* block) {
    size_t row_idx = 0;
    VerticalMergeIteratorContext* pre_ctx = nullptr;
    std::vector<RowSource> tmp_row_sources;
    while (_get_size(block) < _block_row_max) {
        if (_merge_heap.empty()) {
            LOG(INFO) << "_merge_heap empty";
            break;
        }

        auto ctx = _merge_heap.top();
        _merge_heap.pop();
        if (ctx->is_same()) {
            tmp_row_sources.emplace_back(ctx->order(), true);
        } else {
            tmp_row_sources.emplace_back(ctx->order(), false);
        }
        if (ctx->is_same() &&
            (_keys_type == KeysType::UNIQUE_KEYS || _keys_type == KeysType::AGG_KEYS)) {
            // skip cur row, copy pre ctx
            ++_merged_rows;
            if (pre_ctx) {
                pre_ctx->copy_rows(block);
                pre_ctx = nullptr;
            }
        } else {
            ctx->add_cur_batch();
            if (pre_ctx != ctx) {
                if (pre_ctx) {
                    pre_ctx->copy_rows(block);
                }
                pre_ctx = ctx;
            }
            row_idx++;
            if (ctx->is_cur_block_finished() || row_idx >= _block_row_max) {
                // current block finished, ctx not advance
                // so copy start_idx = (_index_in_block - _cur_batch_num + 1)
                ctx->copy_rows(block, false);
                pre_ctx = nullptr;
            }
        }

        RETURN_IF_ERROR(ctx->advance());
        if (ctx->valid()) {
            _merge_heap.push(ctx);
        } else {
            // Release ctx earlier to reduce resource consumed
            delete ctx;
        }
    }
    RETURN_IF_ERROR(_row_sources_buf->append(tmp_row_sources));
    if (!_merge_heap.empty()) {
        return Status::OK();
    }
    return Status::EndOfFile("no more data in segment");
}

Status VerticalHeapMergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema = &(*_origin_iters.begin())->schema();

    auto seg_order = 0;
    for (auto iter : _origin_iters) {
        auto ctx = std::make_unique<VerticalMergeIteratorContext>(iter, _ori_return_cols, seg_order,
                                                                  _seq_col_idx);
        RETURN_IF_ERROR(ctx->init(opts));
        if (!ctx->valid()) {
            continue;
        }
        _merge_heap.push(ctx.release());
        ++seg_order;
    }
    _origin_iters.clear();

    _block_row_max = opts.block_row_max;
    return Status::OK();
}

//  ----------------  VerticalMaskMergeIterator  -------------  //
Status VerticalMaskMergeIterator::next_row(vectorized::IteratorRowRef* ref) {
    DCHECK(_row_sources_buf);
    auto st = _row_sources_buf->has_remaining();
    if (!st.ok()) {
        if (st.is_end_of_file()) {
            for (auto iter : _origin_iter_ctx) {
                RETURN_IF_ERROR(iter->advance());
                DCHECK(!iter->valid());
            }
        }
        return st;
    }
    auto row_source = _row_sources_buf->current();
    uint16_t order = row_source.get_source_num();
    auto& ctx = _origin_iter_ctx[order];
    if (UNLIKELY(ctx->is_first_row())) {
        // first row in block, don't call ctx->advance
        // Except first row, we call advance first and than get cur row
        ctx->set_cur_row_ref(ref);
        ref->is_same = row_source.agg_flag();

        ctx->set_is_first_row(false);
        _row_sources_buf->advance();
        return Status::OK();
    }
    RETURN_IF_ERROR(ctx->advance());
    ctx->set_cur_row_ref(ref);
    ref->is_same = row_source.agg_flag();

    _row_sources_buf->advance();
    return Status::OK();
}

Status VerticalMaskMergeIterator::unique_key_next_row(vectorized::IteratorRowRef* ref) {
    DCHECK(_row_sources_buf);
    auto st = _row_sources_buf->has_remaining();
    while (st.ok()) {
        auto row_source = _row_sources_buf->current();
        uint16_t order = row_source.get_source_num();
        auto& ctx = _origin_iter_ctx[order];
        if (UNLIKELY(ctx->is_first_row()) && !row_source.agg_flag()) {
            // first row in block, don't call ctx->advance
            // Except first row, we call advance first and than get cur row
            ctx->set_cur_row_ref(ref);
            ctx->set_is_first_row(false);
            _row_sources_buf->advance();
            return Status::OK();
        }
        RETURN_IF_ERROR(ctx->advance());
        _row_sources_buf->advance();
        if (!row_source.agg_flag()) {
            ctx->set_cur_row_ref(ref);
            return Status::OK();
        }
        st = _row_sources_buf->has_remaining();
    }
    if (st.is_end_of_file()) {
        for (auto iter : _origin_iter_ctx) {
            RETURN_IF_ERROR(iter->advance());
            DCHECK(!iter->valid());
        }
    }
    return st;
}

Status VerticalMaskMergeIterator::next_batch(Block* block) {
    DCHECK(_row_sources_buf);
    size_t rows = 0;
    auto st = _row_sources_buf->has_remaining();
    while (rows < _block_row_max && st.ok()) {
        uint16_t order = _row_sources_buf->current().get_source_num();
        DCHECK(order < _origin_iter_ctx.size());
        auto& ctx = _origin_iter_ctx[order];

        // find max same source count in cur ctx
        size_t limit = std::min(ctx->remain_rows(), _block_row_max - rows);
        auto same_source_cnt = _row_sources_buf->same_source_count(order, limit);
        _row_sources_buf->advance(same_source_cnt);
        // copy rows to block
        ctx->copy_rows(block, same_source_cnt);
        RETURN_IF_ERROR(ctx->advance());
        rows += same_source_cnt;
        st = _row_sources_buf->has_remaining();
    }
    if (st.is_end_of_file()) {
        for (auto iter : _origin_iter_ctx) {
            RETURN_IF_ERROR(iter->advance());
            DCHECK(!iter->valid());
        }
    }
    return st;
}

Status VerticalMaskMergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema = &(*_origin_iters.begin())->schema();

    for (auto iter : _origin_iters) {
        auto ctx = std::make_unique<VerticalMergeIteratorContext>(iter, _ori_return_cols, -1, -1);
        RETURN_IF_ERROR(ctx->init(opts));
        if (!ctx->valid()) {
            continue;
        }
        _origin_iter_ctx.emplace_back(ctx.release());
    }

    _origin_iters.clear();

    _block_row_max = opts.block_row_max;
    return Status::OK();
}

// interfaces to create vertical merge iterator
std::shared_ptr<RowwiseIterator> new_vertical_heap_merge_iterator(
        const std::vector<RowwiseIterator*>& inputs, size_t ori_return_cols, KeysType keys_type,
        uint32_t seq_col_idx, RowSourcesBuffer* row_sources) {
    return std::make_shared<VerticalHeapMergeIterator>(std::move(inputs), ori_return_cols,
                                                       keys_type, seq_col_idx, row_sources);
}

std::shared_ptr<RowwiseIterator> new_vertical_mask_merge_iterator(
        const std::vector<RowwiseIterator*>& inputs, size_t ori_return_cols,
        RowSourcesBuffer* row_sources) {
    return std::make_shared<VerticalMaskMergeIterator>(std::move(inputs), ori_return_cols,
                                                       row_sources);
}

} // namespace vectorized
} // namespace doris