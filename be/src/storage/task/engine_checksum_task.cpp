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

#include "storage/task/engine_checksum_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "exec/common/sip_hash.h"
#include "io/io_common.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/iterator/block_reader.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/utils.h"

namespace doris {
namespace {

Status prepare_checksum_reader(TabletSharedPtr tablet,
                               const std::vector<RowsetSharedPtr>& input_rowsets,
                               TabletReader::ReaderParams* reader_params, Block* block) {
    reader_params->tablet = tablet;
    reader_params->reader_type = ReaderType::READER_CHECKSUM;
    reader_params->version =
            Version(input_rowsets.front()->start_version(), input_rowsets.back()->end_version());

    TabletReadSource read_source;
    for (const auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        read_source.rs_splits.emplace_back(std::move(rs_reader));
    }
    read_source.fill_delete_predicates();
    reader_params->set_read_source(std::move(read_source));

    std::vector<RowsetMetaSharedPtr> rowset_metas(input_rowsets.size());
    std::transform(input_rowsets.begin(), input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    auto read_tablet_schema = tablet->tablet_schema_with_merged_max_schema_version(rowset_metas);
    reader_params->tablet_schema = read_tablet_schema;
    reader_params->read_schema = std::make_shared<ReadSchema>(read_tablet_schema->columns());
    *block = reader_params->read_schema->create_read_block();

    return Status::OK();
}

} // namespace

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
    BlockReader reader;
    TabletReader::ReaderParams reader_params;
    Block block;
    {
        std::shared_lock rdlock(tablet->get_header_lock());
        auto ret = tablet->capture_consistent_rowsets_unlocked(version, CaptureRowsetOps {});
        if (ret) {
            input_rowsets = std::move(ret->rowsets);
        } else {
            LOG(WARNING) << "fail to captute consistent rowsets. tablet=" << tablet->tablet_id()
                         << "res=" << ret.error();
            return std::move(ret.error());
        }

        RETURN_IF_ERROR(prepare_checksum_reader(tablet, input_rowsets, &reader_params, &block));
    }
    size_t input_size = 0;
    for (const auto& rowset : input_rowsets) {
        input_size += rowset->total_disk_size();
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
