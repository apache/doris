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

#include "segcompaction.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <limits.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include "beta_rowset_writer.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/io_common.h"
#include "olap/data_dir.h"
#include "olap/iterators.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/mem_info.h"
#include "util/time.h"
#include "vec/olap/vertical_block_reader.h"
#include "vec/olap/vertical_merge_iterator.h"

namespace doris {
using namespace ErrorCode;

SegcompactionWorker::SegcompactionWorker(BetaRowsetWriter* writer) : _writer(writer) {}

void SegcompactionWorker::init_mem_tracker(int64_t txn_id) {
    _seg_compact_mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::COMPACTION, "segcompaction-" + std::to_string(txn_id));
}

Status SegcompactionWorker::_get_segcompaction_reader(
        SegCompactionCandidatesSharedPtr segments, TabletSharedPtr tablet,
        std::shared_ptr<Schema> schema, OlapReaderStatistics* stat,
        vectorized::RowSourcesBuffer& row_sources_buf, bool is_key,
        std::vector<uint32_t>& return_columns,
        std::unique_ptr<vectorized::VerticalBlockReader>* reader) {
    auto ctx = _writer->_context;
    bool record_rowids = need_convert_delete_bitmap() && is_key;
    StorageReadOptions read_options;
    read_options.stats = stat;
    read_options.use_page_cache = false;
    read_options.tablet_schema = ctx.tablet_schema;
    read_options.record_rowids = record_rowids;
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    std::map<uint32_t, uint32_t> segment_rows;
    for (auto& seg_ptr : *segments) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(schema, read_options, &iter);
        if (!s.ok()) {
            return Status::Error<INIT_FAILED>("failed to create iterator[{}]: {}", seg_ptr->id(),
                                              s.to_string());
        }
        seg_iterators.push_back(std::move(iter));
        segment_rows.emplace(seg_ptr->id(), seg_ptr->num_rows());
    }
    if (record_rowids && _rowid_conversion != nullptr) {
        _rowid_conversion->reset_segment_map(segment_rows);
    }

    *reader = std::unique_ptr<vectorized::VerticalBlockReader> {
            new vectorized::VerticalBlockReader(&row_sources_buf)};

    TabletReader::ReaderParams reader_params;
    reader_params.is_segcompaction = true;
    reader_params.segment_iters_ptr = &seg_iterators;
    // no reader_params.version shouldn't break segcompaction
    reader_params.tablet_schema = ctx.tablet_schema;
    reader_params.tablet = tablet;
    reader_params.return_columns = return_columns;
    reader_params.is_key_column_group = is_key;
    reader_params.use_page_cache = false;
    reader_params.record_rowids = record_rowids;
    return (*reader)->init(reader_params, nullptr);
}

std::unique_ptr<segment_v2::SegmentWriter> SegcompactionWorker::_create_segcompaction_writer(
        uint32_t begin, uint32_t end) {
    Status status;
    std::unique_ptr<segment_v2::SegmentWriter> writer = nullptr;
    status = _create_segment_writer_for_segcompaction(&writer, begin, end);
    if (!status.ok() || writer == nullptr) {
        LOG(ERROR) << "failed to create segment writer for begin:" << begin << " end:" << end
                   << " status:" << status;
        return nullptr;
    } else {
        return writer;
    }
}

Status SegcompactionWorker::_delete_original_segments(uint32_t begin, uint32_t end) {
    auto fs = _writer->_rowset_meta->fs();
    auto ctx = _writer->_context;
    auto schema = ctx.tablet_schema;
    if (!fs) {
        return Status::Error<INIT_FAILED>(
                "SegcompactionWorker::_delete_original_segments get fs failed");
    }
    for (uint32_t i = begin; i <= end; ++i) {
        auto seg_path = BetaRowset::segment_file_path(ctx.rowset_dir, ctx.rowset_id, i);
        // Even if an error is encountered, these files that have not been cleaned up
        // will be cleaned up by the GC background. So here we only print the error
        // message when we encounter an error.
        RETURN_NOT_OK_STATUS_WITH_WARN(fs->delete_file(seg_path),
                                       strings::Substitute("Failed to delete file=$0", seg_path));
        if (schema->has_inverted_index() &&
            schema->get_inverted_index_storage_format() != InvertedIndexStorageFormatPB::V1) {
            auto idx_path = InvertedIndexDescriptor::get_index_file_name(seg_path);
            VLOG_DEBUG << "segcompaction index. delete file " << idx_path;
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    fs->delete_file(idx_path),
                    strings::Substitute("Failed to delete file=$0", idx_path));
        }
        // Delete inverted index files
        for (auto column : schema->columns()) {
            if (schema->has_inverted_index(*column)) {
                auto index_info = schema->get_inverted_index(*column);
                auto index_id = index_info->index_id();
                auto idx_path = InvertedIndexDescriptor::inverted_index_file_path(
                        ctx.rowset_dir, ctx.rowset_id, i, index_id, index_info->get_index_suffix());
                VLOG_DEBUG << "segcompaction index. delete file " << idx_path;
                if (schema->get_inverted_index_storage_format() ==
                    InvertedIndexStorageFormatPB::V1) {
                    RETURN_NOT_OK_STATUS_WITH_WARN(
                            fs->delete_file(idx_path),
                            strings::Substitute("Failed to delete file=$0", idx_path));
                }
                // Erase the origin index file cache
                RETURN_IF_ERROR(InvertedIndexSearcherCache::instance()->erase(idx_path));
            }
        }
    }
    return Status::OK();
}

Status SegcompactionWorker::_check_correctness(OlapReaderStatistics& reader_stat,
                                               Merger::Statistics& merger_stat, uint32_t begin,
                                               uint32_t end) {
    uint64_t raw_rows_read = reader_stat.raw_rows_read; /* total rows read before merge */
    uint64_t sum_src_row = 0; /* sum of rows in each involved source segments */
    uint64_t filtered_rows = merger_stat.filtered_rows; /* rows filtered by del conditions */
    uint64_t output_rows = merger_stat.output_rows;     /* rows after merge */
    uint64_t merged_rows = merger_stat.merged_rows;     /* dup key merged by unique/agg */

    {
        std::lock_guard<std::mutex> lock(_writer->_segid_statistics_map_mutex);
        for (int i = begin; i <= end; ++i) {
            sum_src_row += _writer->_segid_statistics_map[i].row_num;
        }
    }

    DBUG_EXECUTE_IF("SegcompactionWorker._check_correctness_wrong_sum_src_row", { sum_src_row++; });
    if (raw_rows_read != sum_src_row) {
        return Status::Error<CHECK_LINES_ERROR>(
                "segcompaction read row num does not match source. expect read row:{}, actual read "
                "row:{}",
                sum_src_row, raw_rows_read);
    }

    DBUG_EXECUTE_IF("SegcompactionWorker._check_correctness_wrong_merged_rows", { merged_rows++; });
    if ((output_rows + merged_rows) != raw_rows_read) {
        return Status::Error<CHECK_LINES_ERROR>(
                "segcompaction total row num does not match after merge. expect total row:{},  "
                "actual total row:{}, (output_rows:{},merged_rows:{})",
                raw_rows_read, output_rows + merged_rows, output_rows, merged_rows);
    }
    DBUG_EXECUTE_IF("SegcompactionWorker._check_correctness_wrong_filtered_rows",
                    { filtered_rows++; });
    if (filtered_rows != 0) {
        return Status::Error<CHECK_LINES_ERROR>(
                "segcompaction should not have filtered rows but actual filtered rows:{}",
                filtered_rows);
    }
    return Status::OK();
}

Status SegcompactionWorker::_create_segment_writer_for_segcompaction(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, uint32_t begin, uint32_t end) {
    return _writer->_create_segment_writer_for_segcompaction(writer, begin, end);
}

Status SegcompactionWorker::_do_compact_segments(SegCompactionCandidatesSharedPtr segments) {
    DCHECK(_seg_compact_mem_tracker != nullptr);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_seg_compact_mem_tracker);
    /* throttle segcompaction task if memory depleted */
    if (GlobalMemoryArbitrator::is_exceed_soft_mem_limit(GB_EXCHANGE_BYTE)) {
        return Status::Error<FETCH_MEMORY_EXCEEDED>("skip segcompaction due to memory shortage");
    }

    uint32_t begin = (*(segments->begin()))->id();
    uint32_t end = (*(segments->end() - 1))->id();
    uint64_t begin_time = GetCurrentTimeMicros();
    uint64_t index_size = 0;
    uint64_t total_index_size = 0;
    auto ctx = _writer->_context;

    auto writer = _create_segcompaction_writer(begin, end);
    if (UNLIKELY(writer == nullptr)) {
        return Status::Error<SEGCOMPACTION_INIT_WRITER>("failed to get segcompaction writer");
    }

    DCHECK(ctx.tablet);
    auto tablet = std::static_pointer_cast<Tablet>(ctx.tablet);
    if (need_convert_delete_bitmap() && _rowid_conversion == nullptr) {
        _rowid_conversion = std::make_unique<SimpleRowIdConversion>(_writer->rowset_id());
    }

    std::vector<std::vector<uint32_t>> column_groups;
    Merger::vertical_split_columns(ctx.tablet_schema, &column_groups);
    vectorized::RowSourcesBuffer row_sources_buf(tablet->tablet_id(), tablet->tablet_path(),
                                                 ReaderType::READER_SEGMENT_COMPACTION);

    KeyBoundsPB key_bounds;
    Merger::Statistics key_merger_stats;
    OlapReaderStatistics key_reader_stats;
    /* compact group one by one */
    for (auto i = 0; i < column_groups.size(); ++i) {
        VLOG_NOTICE << "row source size: " << row_sources_buf.total_size();
        bool is_key = (i == 0);
        std::vector<uint32_t> column_ids = column_groups[i];

        writer->clear();
        RETURN_IF_ERROR(writer->init(column_ids, is_key));
        auto schema = std::make_shared<Schema>(ctx.tablet_schema->columns(), column_ids);
        OlapReaderStatistics reader_stats;
        std::unique_ptr<vectorized::VerticalBlockReader> reader;
        auto s = _get_segcompaction_reader(segments, tablet, schema, &reader_stats, row_sources_buf,
                                           is_key, column_ids, &reader);
        if (UNLIKELY(reader == nullptr || !s.ok())) {
            return Status::Error<SEGCOMPACTION_INIT_READER>(
                    "failed to get segcompaction reader. err: {}", s.to_string());
        }

        Merger::Statistics merger_stats;
        RETURN_IF_ERROR(Merger::vertical_compact_one_group(
                tablet, ReaderType::READER_SEGMENT_COMPACTION, ctx.tablet_schema, is_key,
                column_ids, &row_sources_buf, *reader, *writer, &merger_stats, &index_size,
                key_bounds, _rowid_conversion.get()));
        total_index_size += index_size;
        if (is_key) {
            RETURN_IF_ERROR(row_sources_buf.flush());
            key_merger_stats = merger_stats;
            key_reader_stats = reader_stats;
        }
        RETURN_IF_ERROR(row_sources_buf.seek_to_begin());
    }

    /* check row num after merge/aggregation */
    RETURN_NOT_OK_STATUS_WITH_WARN(
            _check_correctness(key_reader_stats, key_merger_stats, begin, end),
            "check correctness failed");
    {
        std::lock_guard<std::mutex> lock(_writer->_segid_statistics_map_mutex);
        _writer->_clear_statistics_for_deleting_segments_unsafe(begin, end);
    }
    RETURN_IF_ERROR(
            _writer->flush_segment_writer_for_segcompaction(&writer, total_index_size, key_bounds));

    if (_file_writer != nullptr) {
        RETURN_IF_ERROR(_file_writer->close());
    }

    RETURN_IF_ERROR(_delete_original_segments(begin, end));
    if (_rowid_conversion != nullptr) {
        convert_segment_delete_bitmap(ctx.mow_context->delete_bitmap, begin, end,
                                      _writer->_num_segcompacted);
    }
    RETURN_IF_ERROR(_writer->_rename_compacted_segments(begin, end));

    if (VLOG_DEBUG_IS_ON) {
        _writer->vlog_buffer.clear();
        for (const auto& entry : std::filesystem::directory_iterator(ctx.rowset_dir)) {
            fmt::format_to(_writer->vlog_buffer, "[{}]", string(entry.path()));
        }
        VLOG_DEBUG << "tablet_id:" << ctx.tablet_id << " rowset_id:" << ctx.rowset_id
                   << "_segcompacted_point:" << _writer->_segcompacted_point
                   << " _num_segment:" << _writer->_num_segment
                   << " _num_segcompacted:" << _writer->_num_segcompacted
                   << " list directory:" << fmt::to_string(_writer->vlog_buffer);
    }

    _writer->_segcompacted_point += (end - begin + 1);

    uint64_t elapsed = GetCurrentTimeMicros() - begin_time;
    LOG(INFO) << "segcompaction completed. tablet_id:" << ctx.tablet_id
              << " rowset_id:" << ctx.rowset_id << " elapsed time:" << elapsed
              << "us. update segcompacted_point:" << _writer->_segcompacted_point
              << " segment num:" << segments->size() << " begin:" << begin << " end:" << end;

    return Status::OK();
}

void SegcompactionWorker::compact_segments(SegCompactionCandidatesSharedPtr segments) {
    Status status = Status::OK();
    if (_is_compacting_state_mutable.exchange(false)) {
        status = _do_compact_segments(segments);
    } else {
        // note: be aware that _writer maybe released when the task is cancelled
        LOG(INFO) << "segcompaction worker is cancelled, skipping segcompaction task";
        return;
    }
    if (!status.ok()) {
        int16_t errcode = status.code();
        switch (errcode) {
        case FETCH_MEMORY_EXCEEDED:
        case SEGCOMPACTION_INIT_READER:
        case SEGCOMPACTION_INIT_WRITER:
            LOG(WARNING) << "segcompaction failed, try next time:" << status;
            break;
        default:
            auto ctx = _writer->_context;
            LOG(WARNING) << "segcompaction fatal, terminating the write job."
                         << " tablet_id:" << ctx.tablet_id << " rowset_id:" << ctx.rowset_id
                         << " status:" << status;
            // status will be checked by the next trigger of segcompaction or the final wait
            _writer->_segcompaction_status.store(ErrorCode::INTERNAL_ERROR);
        }
    }
    DCHECK_EQ(_writer->_is_doing_segcompaction, true);
    {
        std::lock_guard lk(_writer->_is_doing_segcompaction_lock);
        _writer->_is_doing_segcompaction = false;
        _writer->_segcompacting_cond.notify_all();
    }
    _is_compacting_state_mutable = true;
}

bool SegcompactionWorker::need_convert_delete_bitmap() {
    if (_writer == nullptr) {
        return false;
    }
    auto tablet = _writer->context().tablet;
    return tablet != nullptr && tablet->keys_type() == KeysType::UNIQUE_KEYS &&
           tablet->enable_unique_key_merge_on_write() &&
           tablet->tablet_schema()->has_sequence_col();
}

void SegcompactionWorker::convert_segment_delete_bitmap(DeleteBitmapPtr src_delete_bitmap,
                                                        uint32_t src_seg_id, uint32_t dest_seg_id) {
    // lazy init
    if (nullptr == _converted_delete_bitmap) {
        _converted_delete_bitmap = std::make_shared<DeleteBitmap>(_writer->context().tablet_id);
    }
    auto rowset_id = _writer->context().rowset_id;
    const auto* seg_map =
            src_delete_bitmap->get({rowset_id, src_seg_id, DeleteBitmap::TEMP_VERSION_COMMON});
    if (seg_map != nullptr) {
        _converted_delete_bitmap->set({rowset_id, dest_seg_id, DeleteBitmap::TEMP_VERSION_COMMON},
                                      *seg_map);
    }
}

void SegcompactionWorker::convert_segment_delete_bitmap(DeleteBitmapPtr src_delete_bitmap,
                                                        uint32_t src_begin, uint32_t src_end,
                                                        uint32_t dst_seg_id) {
    // lazy init
    if (nullptr == _converted_delete_bitmap) {
        _converted_delete_bitmap = std::make_shared<DeleteBitmap>(_writer->context().tablet_id);
    }
    auto rowset_id = _writer->context().rowset_id;
    RowLocation src(rowset_id, 0, 0);
    for (uint32_t seg_id = src_begin; seg_id <= src_end; seg_id++) {
        const auto* seg_map =
                src_delete_bitmap->get({rowset_id, seg_id, DeleteBitmap::TEMP_VERSION_COMMON});
        if (!seg_map) {
            continue;
        }
        src.segment_id = seg_id;
        for (unsigned int row_id : *seg_map) {
            src.row_id = row_id;
            auto dst_row_id = _rowid_conversion->get(src);
            if (dst_row_id < 0) {
                continue;
            }
            _converted_delete_bitmap->add(
                    {rowset_id, dst_seg_id, DeleteBitmap::TEMP_VERSION_COMMON}, dst_row_id);
        }
    }
}

bool SegcompactionWorker::cancel() {
    // return true if the task is canncellable (actual compaction is not started)
    // return false when the task is not cancellable (it is in the middle of segcompaction)
    return _is_compacting_state_mutable.exchange(false);
}

} // namespace doris
