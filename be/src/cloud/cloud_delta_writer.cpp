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

#include "cloud/cloud_delta_writer.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "olap/delta_writer.h"
#include "runtime/thread_context.h"

namespace doris {

CloudDeltaWriter::CloudDeltaWriter(CloudStorageEngine& engine, const WriteRequest& req,
                                   RuntimeProfile* profile, const UniqueId& load_id)
        : BaseDeltaWriter(req, profile, load_id), _engine(engine) {
    _rowset_builder = std::make_unique<CloudRowsetBuilder>(engine, req, profile);
    _query_thread_context.init_unlocked();
}

CloudDeltaWriter::~CloudDeltaWriter() = default;

Status CloudDeltaWriter::batch_init(std::vector<CloudDeltaWriter*> writers) {
    if (writers.empty()) {
        return Status::OK();
    }

    std::vector<std::function<Status()>> tasks;
    tasks.reserve(writers.size());
    for (auto* writer : writers) {
        if (writer->_is_init || writer->_is_cancelled) {
            continue;
        }

        tasks.emplace_back([writer] {
            SCOPED_ATTACH_TASK(writer->query_thread_context());
            std::lock_guard<bthread::Mutex> lock(writer->_mtx);
            if (writer->_is_init || writer->_is_cancelled) {
                return Status::OK();
            }
            Status st = writer->init(); // included in SCOPED_ATTACH_TASK
            return st;
        });
    }

    return cloud::bthread_fork_join(tasks, 10);
}

Status CloudDeltaWriter::write(const vectorized::Block* block,
                               const std::vector<uint32_t>& row_idxs) {
    if (row_idxs.empty()) [[unlikely]] {
        return Status::OK();
    }
    std::lock_guard lock(_mtx);
    CHECK(_is_init || _is_cancelled);
    {
        SCOPED_TIMER(_wait_flush_limit_timer);
        while (_memtable_writer->flush_running_count() >=
               config::memtable_flush_running_count_limit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return _memtable_writer->write(block, row_idxs);
}

Status CloudDeltaWriter::close() {
    std::lock_guard lock(_mtx);
    CHECK(_is_init);
    return _memtable_writer->close();
}

Status CloudDeltaWriter::cancel_with_status(const Status& st) {
    std::lock_guard lock(_mtx);
    return BaseDeltaWriter::cancel_with_status(st);
}

Status CloudDeltaWriter::build_rowset() {
    std::lock_guard lock(_mtx);
    CHECK(_is_init);
    return BaseDeltaWriter::build_rowset();
}

CloudRowsetBuilder* CloudDeltaWriter::rowset_builder() {
    return static_cast<CloudRowsetBuilder*>(_rowset_builder.get());
}

void CloudDeltaWriter::update_tablet_stats() {
    rowset_builder()->update_tablet_stats();
}

const RowsetMetaSharedPtr& CloudDeltaWriter::rowset_meta() {
    return rowset_builder()->rowset_meta();
}

Status CloudDeltaWriter::commit_rowset() {
    std::lock_guard<bthread::Mutex> lock(_mtx);
    if (!_is_init) {
        // No data to write, but still need to write a empty rowset kv to keep version continuous
        RETURN_IF_ERROR(_rowset_builder->init());
        RETURN_IF_ERROR(_rowset_builder->build_rowset());
    }
    return _engine.meta_mgr().commit_rowset(*rowset_meta());
}

Status CloudDeltaWriter::set_txn_related_delete_bitmap() {
    return rowset_builder()->set_txn_related_delete_bitmap();
}

} // namespace doris
