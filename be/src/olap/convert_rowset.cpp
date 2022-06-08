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

#include "olap/convert_rowset.h"
#include "olap/rowset/rowset_factory.h"

namespace doris {

OLAPStatus ConvertRowset::do_convert() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }
    MutexLock base_compaction_lock(_tablet->get_base_lock(), TRY_LOCK);
    MutexLock cumulative_compaction_lock(_tablet->get_cumulative_lock(), TRY_LOCK);
    if (!base_compaction_lock.own_lock() || !cumulative_compaction_lock.own_lock()) {
        LOG(INFO) << "The tablet is under compaction. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    std::vector<RowsetSharedPtr> alpah_rowsets;
    _tablet->find_alpha_rowsets(&alpah_rowsets);

    Merger::Statistics stats;
    OLAPStatus res = OLAP_SUCCESS;
    const size_t max_convert_row_count = 20000000;
    size_t row_count = 0;
    for (size_t i = 0; i < alpah_rowsets.size(); ++i) {
        Version output_version =
                Version(alpah_rowsets[i]->start_version(), alpah_rowsets[i]->end_version());

        RowsetReaderSharedPtr input_rs_reader;
        RETURN_NOT_OK(alpah_rowsets[i]->create_reader(
                MemTracker::CreateTracker(
                        -1, "ConvertRowset:RowsetReader:" + alpah_rowsets[i]->rowset_id().to_string(),
                        _reader_tracker, true, true),
                &input_rs_reader));

        std::unique_ptr<RowsetWriter> output_rs_writer;
        RETURN_NOT_OK(construct_output_rowset_writer(output_version, &output_rs_writer));

        res = Merger::merge_rowsets(_tablet, ReaderType::READER_BASE_COMPACTION, {input_rs_reader},
                                    output_rs_writer.get(), &stats);

        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to convert rowset. res=" << res
                         << ", tablet=" << _tablet->full_name();
            return res;
        } else {
            auto output_rowset = output_rs_writer->build();
            if (output_rowset == nullptr) {
                LOG(WARNING) << "rowset writer build failed"
                             << ", tablet=" << _tablet->full_name();
                return OLAP_ERR_MALLOC_ERROR;
            }

            RETURN_NOT_OK(check_correctness(alpah_rowsets[i], output_rowset, stats));

            row_count += alpah_rowsets[i]->num_rows();

            RETURN_NOT_OK(_modify_rowsets(alpah_rowsets[i], output_rowset));

            LOG(INFO) << "succeed to convert rowset"
                      << ". tablet=" << _tablet->full_name()
                      << ", output_version=" << output_version
                      << ", disk=" << _tablet->data_dir()->path();

            if (row_count >= max_convert_row_count) {
                break;
            }
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus ConvertRowset::construct_output_rowset_writer(const Version& version, std::unique_ptr<RowsetWriter>* output) {
    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.data_dir = _tablet->data_dir();
    context.rowset_type = _tablet->tablet_meta()->preferred_rowset_type();
    // Alpha Rowset will be removed in the future, so that if the tablet's default rowset type is
    // alpah rowset, then set the newly created rowset to storage engine's default rowset.
    if (context.rowset_type == ALPHA_ROWSET) {
        context.rowset_type = StorageEngine::instance()->default_rowset_type();
    }
    context.path_desc = _tablet->tablet_path_desc();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = version;
    context.segments_overlap = NONOVERLAPPING;
    context.parent_mem_tracker = _writer_tracker;
    // The test results show that one rs writer is low-memory-footprint, there is no need to tracker its mem pool
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(context, output));
    return OLAP_SUCCESS;
}

OLAPStatus ConvertRowset::check_correctness(RowsetSharedPtr input_rowset, RowsetSharedPtr output_rowset,
                                            const Merger::Statistics& stats) {
    // 1. check row number
    if (input_rowset->num_rows() !=
        output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(WARNING) << "row_num does not match between input and output! "
                     << "input_row_num=" << input_rowset->num_rows()
                     << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << output_rowset->num_rows();

        // ATTN(cmy): We found that the num_rows in some rowset meta may be set to the wrong value,
        // but it is not known which version of the code has the problem. So when the convert
        // result is inconsistent, we then try to verify by num_rows recorded in segment_groups.
        // If the check passes, ignore the error and set the correct value in the output rowset meta
        // to fix this problem.
        // Only handle alpha rowset because we only find this bug in alpha rowset
        int64_t num_rows = _get_input_num_rows_from_seg_grps(input_rowset);
        if (num_rows == -1) {
            return OLAP_ERR_CHECK_LINES_ERROR;
        }
        if (num_rows != output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
            // If it is still incorrect, it may be another problem
            LOG(WARNING) << "row_num got from seg groups does not match between cumulative input "
                            "and output! "
                         << "input_row_num=" << num_rows << ", merged_row_num=" << stats.merged_rows
                         << ", filtered_row_num=" << stats.filtered_rows
                         << ", output_row_num=" << output_rowset->num_rows();

            return OLAP_ERR_CHECK_LINES_ERROR;
        }
    }
    return OLAP_SUCCESS;
}

int64_t ConvertRowset::_get_input_num_rows_from_seg_grps(RowsetSharedPtr rowset) {
    int64_t num_rows = 0;
    for (auto& seg_grp : rowset->rowset_meta()->alpha_rowset_extra_meta_pb().segment_groups()) {
        num_rows += seg_grp.num_rows();
    }
    return num_rows;
}
OLAPStatus ConvertRowset::_modify_rowsets(RowsetSharedPtr input_rowset, RowsetSharedPtr output_rowset) {
    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.push_back(input_rowset);

    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(output_rowset);

    WriteLock wrlock(_tablet->get_header_lock());
    RETURN_NOT_OK(_tablet->modify_rowsets(output_rowsets, input_rowsets, true));
    _tablet->save_meta();
    return OLAP_SUCCESS;
}
} // namespace doris