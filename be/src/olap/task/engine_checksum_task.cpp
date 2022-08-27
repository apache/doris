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

#include "olap/task/engine_checksum_task.h"

#include "olap/row.h"
#include "olap/tuple_reader.h"
#include "runtime/thread_context.h"
#include "vec/olap/block_reader.h"

namespace doris {

EngineChecksumTask::EngineChecksumTask(TTabletId tablet_id, TSchemaHash schema_hash,
                                       TVersion version, uint32_t* checksum)
        : _tablet_id(tablet_id), _schema_hash(schema_hash), _version(version), _checksum(checksum) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(
            -1, "EngineChecksumTask#tabletId=" + std::to_string(tablet_id),
            StorageEngine::instance()->consistency_mem_tracker());
}

Status EngineChecksumTask::execute() {
    SCOPED_ATTACH_TASK(_mem_tracker, ThreadContext::TaskType::STORAGE);
    return _compute_checksum();
} // execute

Status EngineChecksumTask::_compute_checksum() {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
              << ", version=" << _version;
    Status res = Status::OK();

    if (_checksum == nullptr) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        return Status::OLAPInternalError(OLAP_ERR_CE_CMD_PARAMS_ERROR);
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    if (nullptr == tablet.get()) {
        LOG(WARNING) << "can't find tablet. [tablet_id=" << _tablet_id
                     << " schema_hash=" << _schema_hash << "]";
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    std::vector<RowsetSharedPtr> input_rowsets;
    Version version(0, _version);
    Status acquire_reader_st = tablet->capture_consistent_rowsets(version, &input_rowsets);
    if (acquire_reader_st != Status::OK()) {
        LOG(WARNING) << "fail to captute consistent rowsets. tablet=" << tablet->full_name()
                     << "res=" << acquire_reader_st;
        return acquire_reader_st;
    }

    vectorized::BlockReader reader;
    TabletReader::ReaderParams reader_params;
    vectorized::Block block;
    RETURN_NOT_OK(TabletReader::init_reader_params_and_create_block(
            tablet, READER_CHECKSUM, input_rowsets, &reader_params, &block));

    res = reader.init(reader_params);
    if (!res.ok()) {
        LOG(WARNING) << "initiate reader fail. res = " << res;
        return res;
    }

    bool eof = false;
    uint32_t row_checksum = 0;
    SipHash block_hash;
    uint64_t rows = 0;
    while (!eof) {
        res = reader.next_block_with_aggregation(&block, nullptr, nullptr, &eof);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read in reader. res = " << res;
            return res;
        }
        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "block " << block.dump_data();
        }
        block.update_hash(block_hash);
        rows += block.rows();
        block.clear_column_data();
    }

    LOG(INFO) << "success to finish compute checksum. tablet_id = " << _tablet_id
              << ", rows = " << rows << ", checksum=" << row_checksum;
    uint64_t checksum64 = block_hash.get64();
    *_checksum = (checksum64 >> 32) ^ (checksum64 & 0xffffffff);
    return Status::OK();
}

} // namespace doris
