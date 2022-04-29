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
#include "runtime/thread_context.h"
#include "olap/tuple_reader.h"

namespace doris {

EngineChecksumTask::EngineChecksumTask(TTabletId tablet_id, TSchemaHash schema_hash,
                                       TVersion version, uint32_t* checksum)
        : _tablet_id(tablet_id), _schema_hash(schema_hash), _version(version), _checksum(checksum) {
    _mem_tracker = MemTracker::create_tracker(-1, "compute checksum: " + std::to_string(tablet_id),
                                              StorageEngine::instance()->consistency_mem_tracker(),
                                              MemTrackerLevel::TASK);
}

Status EngineChecksumTask::execute() {
    SCOPED_ATTACH_TASK_THREAD(ThreadContext::TaskType::STORAGE, _mem_tracker);
    return _compute_checksum();
} // execute

Status EngineChecksumTask::_compute_checksum() {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
              << ", version=" << _version;
    Status res = Status::OK();

    if (_checksum == nullptr) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return Status::OLAPInternalError(OLAP_ERR_CE_CMD_PARAMS_ERROR);
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    if (nullptr == tablet.get()) {
        OLAP_LOG_WARNING("can't find tablet. [tablet_id=%ld schema_hash=%d]", _tablet_id,
                         _schema_hash);
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    TupleReader reader;
    TabletReader::ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = READER_CHECKSUM;
    reader_params.version = Version(0, _version);

    {
        std::shared_lock rdlock(tablet->get_header_lock());
        const RowsetSharedPtr message = tablet->rowset_with_max_version();
        if (message == nullptr) {
            LOG(FATAL) << "fail to get latest version. tablet_id=" << _tablet_id;
            return Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
        }

        Status acquire_reader_st =
                tablet->capture_rs_readers(reader_params.version, &reader_params.rs_readers);
        if (acquire_reader_st != Status::OK()) {
            LOG(WARNING) << "fail to init reader. tablet=" << tablet->full_name()
                         << "res=" << acquire_reader_st;
            return acquire_reader_st;
        }
    }

    for (size_t i = 0; i < tablet->tablet_schema().num_columns(); ++i) {
        reader_params.return_columns.push_back(i);
    }

    res = reader.init(reader_params);
    if (!res.ok()) {
        LOG(WARNING) << "initiate reader fail. res = " << res;
        return res;
    }

    RowCursor row;
    std::unique_ptr<MemPool> mem_pool(new MemPool("EngineChecksumTask:_compute_checksum"));
    std::unique_ptr<ObjectPool> agg_object_pool(new ObjectPool());
    res = row.init(tablet->tablet_schema(), reader_params.return_columns);
    if (!res.ok()) {
        LOG(WARNING) << "failed to init row cursor. res = " << res;
        return res;
    }
    row.allocate_memory_for_string_type(tablet->tablet_schema());

    bool eof = false;
    uint32_t row_checksum = 0;
    while (true) {
        Status res =
                reader.next_row_with_aggregation(&row, mem_pool.get(), agg_object_pool.get(), &eof);
        if (res.ok() && eof) {
            VLOG_NOTICE << "reader reads to the end.";
            break;
        } else if (!res.ok()) {
            LOG(WARNING) << "fail to read in reader. res = " << res;
            return res;
        }
        // The value of checksum is independent of the sorting of data rows.
        row_checksum ^= hash_row(row, 0);
        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
        agg_object_pool.reset(new ObjectPool());
    }

    LOG(INFO) << "success to finish compute checksum. checksum=" << row_checksum;
    *_checksum = row_checksum;
    return Status::OK();
}

} // namespace doris
