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

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <vector>

#include "io/io_common.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_reader.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "vec/common/sip_hash.h"
#include "vec/core/block.h"
#include "vec/olap/block_reader.h"

namespace doris {

EngineChecksumTask::EngineChecksumTask(StorageEngine& engine, TTabletId tablet_id,
                                       TSchemaHash schema_hash, TVersion version,
                                       uint32_t* checksum)
        : _engine(engine),
          _tablet_id(tablet_id),
          _schema_hash(schema_hash),
          _version(version),
          _checksum(checksum) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::LOAD,
            "EngineChecksumTask#tabletId=" + std::to_string(tablet_id));
}

EngineChecksumTask::~EngineChecksumTask() = default;

Status EngineChecksumTask::execute() {
    return _compute_checksum();
} // execute

Status EngineChecksumTask::_compute_checksum() {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
              << ", version=" << _version;
    OlapStopWatch watch;

    if (_checksum == nullptr) {
        return Status::InvalidArgument("invalid checksum which is nullptr");
    }

    TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(_tablet_id);
    if (nullptr == tablet) {
        return Status::InternalError("could not find tablet {}", _tablet_id);
    }

    std::vector<RowsetSharedPtr> input_rowsets;
    Version version(0, _version);
    vectorized::BlockReader reader;
    TabletReader::ReaderParams reader_params;
    vectorized::Block block;
    {
        std::shared_lock rdlock(tablet->get_header_lock());
        Status acquire_reader_st =
                tablet->capture_consistent_rowsets_unlocked(version, &input_rowsets);
        if (!acquire_reader_st.ok()) {
            LOG(WARNING) << "fail to captute consistent rowsets. tablet=" << tablet->tablet_id()
                         << "res=" << acquire_reader_st;
            return acquire_reader_st;
        }
        RETURN_IF_ERROR(TabletReader::init_reader_params_and_create_block(
                tablet, ReaderType::READER_CHECKSUM, input_rowsets, &reader_params, &block));
    }
    size_t input_size = 0;
    for (const auto& rowset : input_rowsets) {
        input_size += rowset->data_disk_size();
    }

    auto res = reader.init(reader_params);
    if (!res.ok()) {
        LOG(WARNING) << "initiate reader fail. res = " << res;
        return res;
    }

    bool eof = false;
    SipHash block_hash;
    uint64_t rows = 0;
    while (!eof) {
        RETURN_IF_ERROR(reader.next_block_with_aggregation(&block, &eof));
        rows += block.rows();

        block.update_hash(block_hash);
        block.clear_column_data();
    }
    uint64_t checksum64 = block_hash.get64();
    *_checksum = (checksum64 >> 32) ^ (checksum64 & 0xffffffff);

    LOG(INFO) << "success to finish compute checksum. tablet_id = " << _tablet_id
              << ", rows = " << rows << ", checksum=" << *_checksum
              << ", total_size = " << input_size << ", cost(us): " << watch.get_elapse_time_us();
    return Status::OK();
}

} // namespace doris
