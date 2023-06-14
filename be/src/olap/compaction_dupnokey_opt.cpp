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

#include "olap/compaction_dupnokey_opt.h"

#include <gen_cpp/types.pb.h>

#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/storage_engine.h"
#include "util/trace.h"
#include "vec/olap/vertical_block_reader.h"
#include "vec/olap/vertical_merge_iterator.h"

namespace doris {
using namespace ErrorCode;
Status CompactionDupnokeyOpt::handle_duplicate_nokey_compaction() {
    std::map<RowsetSharedPtr, std::vector<size_t>> to_compacts, to_links;
    size_t link_segment_file_size = 0, compact_segment_file_size = 0, compact_segments = 0,
           link_segments = 0;
    if (!_duplicate_nokey_check_and_split_segments(to_compacts, to_links, compact_segments,
                                                   compact_segment_file_size, link_segments,
                                                   link_segment_file_size)) {
        return Status::NotSupported("Too small");
    }

    _parent.build_basic_info();

    RETURN_IF_ERROR(_parent.construct_output_rowset_writer(_ctx, true));
    Merger::Statistics stats;
    if (!to_compacts.empty()) {
        Status res = _do_compact_segments(to_compacts, &stats);
        if (!res.ok()) {
            LOG(WARNING) << "fail to do " << _parent.compaction_name() << ". res=" << res
                         << ", tablet=" << _parent._tablet->full_name()
                         << ", output_version=" << _parent._output_version;
            return res;
        }
        VLOG(1) << "Dupnokeyopt merge [" << _parent.compaction_name()
                << "] tablet:" << _parent._tablet->full_name()
                << " finished, merge segments : " << compact_segments
                << " merge file size: " << compact_segment_file_size
                << " merge_rows:" << stats.merged_rows << " filtered_rows:" << stats.filtered_rows;

        TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
        TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);
    } else {
        VLOG(1) << "Dupnokeyopt merge [" << _parent.compaction_name()
                << "] tablet:" << _parent._tablet->full_name()
                << "] not needed, merge segments : " << compact_segments
                << " merge file size: " << compact_segment_file_size;
    }

    // TODO: will get newest seg id from the writer
    ///size_t newest_seg_id = _parent._output_rowset->num_segments();

    VLOG(1) << "Dupnokeyopt link [" << _parent.compaction_name()
            << "] tablet:" << _parent._tablet->full_name() << " segments : " << link_segments
            << ", link file size: " << link_segment_file_size;
    Status res = _do_compact_duplicate_nokey_over_size(to_links, link_segment_file_size);
    if (!res.ok()) {
        return res;
    }
    _parent._output_rowset = _parent._output_rs_writer->build();
    if (_parent._output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << ", output_version=" << _parent._output_version;
        return Status::Error<ROWSET_BUILDER_INIT>();
    }

    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _parent._output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _parent._output_rowset->num_rows());
    return Status::OK();
    TRACE_COUNTER_INCREMENT("output_segments_num", _parent._output_rowset->num_segments());
}

Status CompactionDupnokeyOpt::_do_compact_segments(
        const std::map<RowsetSharedPtr, std::vector<size_t>>& to_compacts,
        Merger::Statistics* output_stats) {
    auto tablet = _parent._tablet;

    std::vector<std::vector<uint32_t>> column_groups;
    Merger::vertical_split_columns(_ctx.tablet_schema, &column_groups);
    vectorized::RowSourcesBuffer row_sources_buf(tablet->tablet_id(), tablet->tablet_path(),
                                                 ReaderType::READER_SEGMENT_COMPACTION);

    KeyBoundsPB key_bounds;
    Merger::Statistics key_merger_stats;
    OlapReaderStatistics key_reader_stats;

    int64_t max_rows_per_segments = _parent.get_avg_segment_rows();
    /* compact group one by one */
    for (auto i = 0; i < column_groups.size(); ++i) {
        VLOG_NOTICE << "row source size: " << row_sources_buf.total_size();
        bool is_key = (i == 0);
        std::vector<uint32_t> column_ids = column_groups[i];
        auto schema = std::make_shared<Schema>(_ctx.tablet_schema->columns(), column_ids);
        OlapReaderStatistics reader_stats;
        std::unique_ptr<vectorized::VerticalBlockReader> reader;
        auto s = _get_segcompaction_reader(to_compacts, tablet, schema, &reader_stats,
                                           row_sources_buf, is_key, column_ids, &reader);
        if (UNLIKELY(reader == nullptr || !s.ok())) {
            LOG(WARNING) << "failed to get segment reader";
            return Status::Error<SEGCOMPACTION_INIT_READER>();
        }

        RETURN_IF_ERROR(vertical_compact_one_group(tablet, ReaderType::READER_SEGMENT_COMPACTION,
                                                   _ctx.tablet_schema, is_key, column_ids, *reader,
                                                   _parent._output_rs_writer.get(),
                                                   max_rows_per_segments, &key_merger_stats));
        if (is_key) {
            row_sources_buf.flush();
        }
        row_sources_buf.seek_to_begin();
    }
    // finish compact, build output rowset
    VLOG_NOTICE << "finish compact groups";
    RETURN_IF_ERROR(_parent._output_rs_writer->final_flush());

    return Status::OK();
}

Status CompactionDupnokeyOpt::_get_segcompaction_reader(
        const std::map<RowsetSharedPtr, std::vector<size_t>>& to_compacts, TabletSharedPtr tablet,
        std::shared_ptr<Schema> schema, OlapReaderStatistics* stat,
        vectorized::RowSourcesBuffer& row_sources_buf, bool is_key,
        std::vector<uint32_t>& return_columns,
        std::unique_ptr<vectorized::VerticalBlockReader>* reader) {
    StorageReadOptions read_options;
    read_options.stats = stat;
    read_options.use_page_cache = false;
    read_options.tablet_schema = _ctx.tablet_schema;
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& to_compact : to_compacts) {
        auto beta_rowset = reinterpret_cast<BetaRowset*>(to_compact.first.get());
        for (size_t seg_id : to_compact.second) {
            std::unique_ptr<RowwiseIterator> iter;
            segment_v2::SegmentSharedPtr seg_ptr;
            auto s = beta_rowset->load_segment(seg_id, seg_ptr);
            if (!s.ok()) {
                LOG(WARNING) << "failed to load segment[" << beta_rowset->rowset_id() << ":"
                             << seg_ptr->id() << "]: " << s.to_string();
                return Status::Error<INIT_FAILED>();
            }
            s = seg_ptr->new_iterator(schema, read_options, &iter);
            if (!s.ok()) {
                LOG(WARNING) << "failed to create iterator[" << seg_ptr->id()
                             << "]: " << s.to_string();
                return Status::Error<INIT_FAILED>();
            }
            seg_iterators.push_back(std::move(iter));
        }
    }

    *reader = std::unique_ptr<vectorized::VerticalBlockReader> {
            new vectorized::VerticalBlockReader(&row_sources_buf)};

    TabletReader::ReaderParams reader_params;
    reader_params.is_segcompaction = true;
    reader_params.segment_iters_ptr = &seg_iterators;
    // no reader_params.version shouldn't break segcompaction
    reader_params.tablet_schema = _ctx.tablet_schema;
    reader_params.tablet = tablet;
    reader_params.return_columns = return_columns;
    reader_params.is_key_column_group = is_key;
    return (*reader)->init(reader_params);
}

// return link segments' total size
bool CompactionDupnokeyOpt::_duplicate_nokey_check_and_split_segments(
        std::map<RowsetSharedPtr, std::vector<size_t>>& to_compacts,
        std::map<RowsetSharedPtr, std::vector<size_t>>& to_links, size_t& compact_segments,
        size_t& compact_segment_file_size, size_t& link_segments, size_t& link_segment_file_size) {
    size_t min_tidy_size = config::dup_without_key_opt_compaction_min_segment_size;

    for (auto& rowset : _parent._input_rowsets) {
        std::vector<size_t> segments_size;
        auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
        beta_rowset->get_segments_size(&segments_size);
        for (size_t i = 0; i < segments_size.size(); i++) {
            if (segments_size[i] >= min_tidy_size) {
                to_links[rowset].push_back(i);
                ++link_segments;
                link_segment_file_size += segments_size[i];
            } else {
                to_compacts[rowset].push_back(i);
                ++compact_segments;
                compact_segment_file_size += segments_size[i];
            }
        }
    }
    if (compact_segments == 1) {
        auto it = to_compacts.begin();
        to_links[it->first].push_back(*it->second.begin());
        to_compacts.clear();
        link_segment_file_size += compact_segment_file_size;
        compact_segment_file_size = 0;
    }
    return true;
}

Status CompactionDupnokeyOpt::_do_compact_duplicate_nokey_over_size(
        std::map<RowsetSharedPtr, std::vector<size_t>>& to_links, size_t link_segment_file_size) {
    LOG(INFO) << "start to do oversize data compaction, tablet=" << _parent._tablet->full_name()
              << ", output_version=" << _parent._output_version
              << ", link size=" << to_links.size();
    // link data to new rowset
    KeyBoundsPB empty_key_bound; // TODO: duplicate only need empty key_bound;
    for (auto to_link : to_links) {
        for (size_t seg_id : to_link.second) {
            auto beta_rowset = reinterpret_cast<BetaRowset*>(to_link.first.get());
            segment_v2::SegmentSharedPtr seg_ptr;
            RETURN_IF_ERROR(beta_rowset->load_segment(seg_id, seg_ptr));
            RETURN_IF_ERROR(_parent._output_rs_writer->add_segment(to_link.first, seg_ptr));
        }
    }
    return Status::OK();
}

Status CompactionDupnokeyOpt::vertical_compact_one_group(
        TabletSharedPtr tablet, ReaderType reader_type, TabletSchemaSPtr tablet_schema, bool is_key,
        const std::vector<uint32_t>& column_group, vectorized::VerticalBlockReader& reader,
        RowsetWriter* dst_rowset_writer, int64_t max_rows_per_segment,
        Merger::Statistics* stats_output) {
    // build tablet reader
    VLOG_NOTICE << "vertical compact one group, max_rows_per_segment=" << max_rows_per_segment;
    // duplicate table need not to consider rowid_conversion

    vectorized::Block block = tablet_schema->create_block(column_group);
    size_t output_rows = 0;
    bool eof = false;
    while (!eof && !StorageEngine::instance()->stopped()) {
        // Read one block from block reader
        RETURN_NOT_OK_STATUS_WITH_WARN(
                reader.next_block_with_aggregation(&block, &eof),
                "failed to read next block when merging rowsets of tablet " + tablet->full_name());
        RETURN_NOT_OK_STATUS_WITH_WARN(
                dst_rowset_writer->add_columns(&block, column_group, is_key, max_rows_per_segment),
                "failed to write block when merging rowsets of tablet " + tablet->full_name());

        output_rows += block.rows();
        block.clear_column_data();
    }
    if (StorageEngine::instance()->stopped()) {
        LOG(INFO) << "tablet " << tablet->full_name() << "failed to do compaction, engine stopped";
        return Status::Error<doris::ErrorCode::INTERNAL_ERROR>();
    }

    if (is_key && stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }
    RETURN_IF_ERROR(dst_rowset_writer->flush_columns(is_key));

    return Status::OK();
}

} // namespace doris
