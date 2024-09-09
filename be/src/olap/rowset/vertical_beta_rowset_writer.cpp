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

#include "olap/rowset/vertical_beta_rowset_writer.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include "cloud/cloud_rowset_writer.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer_context.h"
#include "util/slice.h"
#include "util/spinlock.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

template class VerticalBetaRowsetWriter<BetaRowsetWriter>;
template class VerticalBetaRowsetWriter<CloudRowsetWriter>;

template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
Status VerticalBetaRowsetWriter<T>::add_columns(const vectorized::Block* block,
                                                const std::vector<uint32_t>& col_ids, bool is_key,
                                                uint32_t max_rows_per_segment) {
    auto& context = this->_context;

    VLOG_NOTICE << "VerticalBetaRowsetWriter::add_columns, columns: " << block->columns();
    size_t num_rows = block->rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    if (UNLIKELY(max_rows_per_segment > context.max_rows_per_segment)) {
        max_rows_per_segment = context.max_rows_per_segment;
    }

    if (_segment_writers.empty()) {
        // it must be key columns
        DCHECK(is_key);
        std::unique_ptr<segment_v2::SegmentWriter> writer;
        RETURN_IF_ERROR(_create_segment_writer(col_ids, is_key, &writer));
        _segment_writers.emplace_back(std::move(writer));
        _cur_writer_idx = 0;
        RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->append_block(block, 0, num_rows));
    } else if (is_key) {
        if (_segment_writers[_cur_writer_idx]->num_rows_written() > max_rows_per_segment) {
            // segment is full, need flush columns and create new segment writer
            RETURN_IF_ERROR(_flush_columns(_segment_writers[_cur_writer_idx].get(), true));

            std::unique_ptr<segment_v2::SegmentWriter> writer;
            RETURN_IF_ERROR(_create_segment_writer(col_ids, is_key, &writer));
            _segment_writers.emplace_back(std::move(writer));
            ++_cur_writer_idx;
        }
        RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->append_block(block, 0, num_rows));
    } else {
        // value columns
        int64_t left = num_rows;
        while (left > 0) {
            uint32_t num_rows_written = _segment_writers[_cur_writer_idx]->num_rows_written();
            VLOG_NOTICE << "num_rows_written: " << num_rows_written
                        << ", _cur_writer_idx: " << _cur_writer_idx;
            uint32_t num_rows_key_group = _segment_writers[_cur_writer_idx]->row_count();
            CHECK_LT(num_rows_written, num_rows_key_group);
            // init if it's first value column write in current segment
            if (num_rows_written == 0) {
                VLOG_NOTICE << "init first value column segment writer";
                RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->init(col_ids, is_key));
            }

            int64_t to_write = num_rows_written + left >= num_rows_key_group
                                       ? num_rows_key_group - num_rows_written
                                       : left;
            RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->append_block(block, num_rows - left,
                                                                            to_write));
            left -= to_write;
            CHECK_GE(left, 0);

            if (num_rows_key_group == num_rows_written + to_write &&
                _cur_writer_idx < _segment_writers.size() - 1) {
                RETURN_IF_ERROR(_flush_columns(_segment_writers[_cur_writer_idx].get()));
                ++_cur_writer_idx;
            }
        }
    }
    if (is_key) {
        this->_num_rows_written += num_rows;
    }
    return Status::OK();
}

template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
Status VerticalBetaRowsetWriter<T>::_flush_columns(segment_v2::SegmentWriter* segment_writer,
                                                   bool is_key) {
    uint64_t index_size = 0;
    VLOG_NOTICE << "flush columns index: " << _cur_writer_idx;
    RETURN_IF_ERROR(segment_writer->finalize_columns_data());
    RETURN_IF_ERROR(segment_writer->finalize_columns_index(&index_size));
    if (is_key) {
        _total_key_group_rows += segment_writer->row_count();
        // record segment key bound
        KeyBoundsPB key_bounds;
        Slice min_key = segment_writer->min_encoded_key();
        Slice max_key = segment_writer->max_encoded_key();
        DCHECK_LE(min_key.compare(max_key), 0);
        key_bounds.set_min_key(min_key.to_string());
        key_bounds.set_max_key(max_key.to_string());
        this->_segments_encoded_key_bounds.emplace_back(std::move(key_bounds));
        this->_segment_num_rows.resize(_cur_writer_idx + 1);
        this->_segment_num_rows[_cur_writer_idx] = _segment_writers[_cur_writer_idx]->row_count();
    }
    this->_total_index_size += static_cast<int64_t>(index_size);
    return Status::OK();
}

template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
Status VerticalBetaRowsetWriter<T>::flush_columns(bool is_key) {
    if (_segment_writers.empty()) {
        return Status::OK();
    }

    DCHECK(_cur_writer_idx < _segment_writers.size() && _segment_writers[_cur_writer_idx]);
    RETURN_IF_ERROR(_flush_columns(_segment_writers[_cur_writer_idx].get(), is_key));
    _cur_writer_idx = 0;
    return Status::OK();
}

template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
Status VerticalBetaRowsetWriter<T>::_create_segment_writer(
        const std::vector<uint32_t>& column_ids, bool is_key,
        std::unique_ptr<segment_v2::SegmentWriter>* writer) {
    auto& context = this->_context;

    int seg_id = this->_num_segment.fetch_add(1, std::memory_order_relaxed);

    io::FileWriterPtr file_writer;
    io::FileWriterOptions opts = this->_context.get_file_writer_options();

    auto path = context.segment_path(seg_id);
    auto& fs = context.fs_ref();
    Status st = fs.create_file(path, &file_writer, &opts);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create writable file. path=" << path << ", err: " << st;
        return st;
    }

    DCHECK(file_writer != nullptr);
    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &context;
    writer_options.max_rows_per_segment = context.max_rows_per_segment;
    *writer = std::make_unique<segment_v2::SegmentWriter>(file_writer.get(), seg_id,
                                                          context.tablet_schema, context.tablet,
                                                          context.data_dir, writer_options);
    RETURN_IF_ERROR(this->_seg_files.add(seg_id, std::move(file_writer)));

    auto s = (*writer)->init(column_ids, is_key);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer->reset(nullptr);
        return s;
    }
    return Status::OK();
}

template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
Status VerticalBetaRowsetWriter<T>::final_flush() {
    for (auto& segment_writer : _segment_writers) {
        uint64_t segment_size = 0;
        //uint64_t footer_position = 0;
        auto st = segment_writer->finalize_footer(&segment_size);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to finalize segment footer, " << st;
            return st;
        }
        this->_total_data_size += segment_size + segment_writer->get_inverted_index_total_size();
        this->_total_index_size += segment_writer->get_inverted_index_total_size();
        this->_idx_files_info.add_file_info(segment_writer->get_segment_id(),
                                            segment_writer->get_inverted_index_file_info());
        segment_writer.reset();
    }
    return Status::OK();
}

template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
Status VerticalBetaRowsetWriter<T>::_close_file_writers() {
    return this->_seg_files.close();
}

} // namespace doris
