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

#include "olap/task/engine_cloud_index_change_task.h"

#include "cloud/cloud_index_change_compaction.h"
#include "cloud/cloud_tablet_mgr.h"
#include "olap/tablet_manager.h"

namespace doris {

EngineCloudIndexChangeTask::EngineCloudIndexChangeTask(CloudStorageEngine& engine,
                                                     const TAlterInvertedIndexReq& request)
        : _engine(engine),
          _alter_inverted_indexes(request.alter_inverted_indexes),
          _tablet_id(request.tablet_id),
          _is_drop(request.is_drop_op) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::SCHEMA_CHANGE,
            fmt::format("EngineCloudIndexChangeTask#tabletId={}", std::to_string(_tablet_id)),
            engine.memory_limitation_bytes_per_thread_for_schema_change());
}

EngineCloudIndexChangeTask::~EngineCloudIndexChangeTask() = default;

TabletSchemaSPtr EngineCloudIndexChangeTask::build_output_rs_index_schema_for_drop(
        const TabletSchemaSPtr& input_rs_tablet_schema) {
    TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
    output_rs_tablet_schema->copy_from(*input_rs_tablet_schema);
    for (const auto& t_inverted_index : _alter_inverted_indexes) {
        DCHECK_EQ(t_inverted_index.columns.size(), 1);
        auto column_name = t_inverted_index.columns[0];
        auto column_idx = output_rs_tablet_schema->field_index(column_name);
        if (column_idx < 0) {
            if (!t_inverted_index.column_unique_ids.empty()) {
                auto column_unique_id = t_inverted_index.column_unique_ids[0];
                column_idx = output_rs_tablet_schema->field_index(column_unique_id);
            }
            if (column_idx < 0) {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_idx
                             << "]";
                continue;
            }
        }
        auto column = output_rs_tablet_schema->column(column_idx);
        const auto* index_meta = output_rs_tablet_schema->inverted_index(column);
        if (index_meta == nullptr) {
            LOG(ERROR) << "failed to find column: " << column_name
                       << " index_id: " << t_inverted_index.index_id;
            continue;
        }
        output_rs_tablet_schema->remove_index(index_meta->index_id());
    }

    return output_rs_tablet_schema;
}

TabletSchemaSPtr EngineCloudIndexChangeTask::build_output_rs_index_schema_for_add(
        const TabletSchemaSPtr& input_rs_tablet_schema) {
    TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
    output_rs_tablet_schema->copy_from(*input_rs_tablet_schema);

    for (auto t_inverted_index : _alter_inverted_indexes) {
        TabletIndex index;
        index.init_from_thrift(t_inverted_index, *output_rs_tablet_schema);
        auto column_uid = index.col_unique_ids()[0];
        if (column_uid < 0) {
            LOG(WARNING) << "referenced column was missing. "
                         << "[column=" << t_inverted_index.columns[0]
                         << " referenced_column=" << column_uid << "]";
            continue;
        }
        const TabletColumn& col = output_rs_tablet_schema->column_by_uid(column_uid);
        const TabletIndex* exist_index = output_rs_tablet_schema->inverted_index(col);
        if (exist_index && exist_index->index_id() != index.index_id()) {
            LOG(WARNING) << fmt::format(
                    "column: {} has a exist inverted index, but the index id not equal "
                    "request's index id, , exist index id: {}, request's index id: {}, "
                    "remove exist index in new output_rs_tablet_schema",
                    column_uid, exist_index->index_id(), index.index_id());
            output_rs_tablet_schema->remove_index(exist_index->index_id());
        }
        output_rs_tablet_schema->append_index(std::move(index));
    }
    return output_rs_tablet_schema;
}

Status EngineCloudIndexChangeTask::execute() {
    std::set<int64_t> alter_index_ids;
    for (auto inverted_index : _alter_inverted_indexes) {
        alter_index_ids.insert(inverted_index.index_id);
    }

    while (true) {
        // todo: add timeout exit
        auto result = _engine.tablet_mgr().get_tablet(_tablet_id);
        CloudTabletSPtr tablet = result.value();
        DBUG_EXECUTE_IF("StorageEngine::process_index_change_task_tablet_nullptr",
                        { tablet = nullptr; })
        if (tablet == nullptr) {
            LOG(WARNING) << "[log0630]tablet: " << _tablet_id << " not exist";
            return Status::InternalError("tablet not exist, tablet_id={}.", _tablet_id);
        }

        Status st = tablet->sync_rowsets();
        if (!st.ok()) {
            LOG(INFO) << "[log0630] sync rowset failed";
        }

        RowsetSharedPtr input_rowset =
                tablet->pick_a_rowset_for_index_change(alter_index_ids, _is_drop);
        if (input_rowset == nullptr) {
            LOG(INFO) << "[log0630] can not find a rowset";
            return Status::OK();
        }

        TabletSchemaSPtr output_schema = nullptr;
        if (_is_drop) {
            LOG(INFO) << "[log0630] build drop schema";
            output_schema = build_output_rs_index_schema_for_drop(input_rowset->tablet_schema());
        } else {
            LOG(INFO) << "[log0630] build add schema";
            output_schema = build_output_rs_index_schema_for_add(input_rowset->tablet_schema());
        }

        std::shared_ptr<CloudIndexChangeCompaction> index_change_compact =
                std::make_shared<CloudIndexChangeCompaction>(_engine, tablet, input_rowset,
                                                            output_schema);

        bool is_register_succ =
                _engine.register_index_change_compaction(index_change_compact, _tablet_id);
        if (!is_register_succ) {
            LOG(INFO) << "[log0630] regist compaction failed";
            // todo add sleep here.
            continue;
        }

        Defer defer {[&]() {
            _engine.unregister_index_change_compaction(_tablet_id);
            LOG(INFO) << "[log0630] unregist compaction , is drop:" << ((int)_is_drop);
        }};

        LOG(INFO) << "[log0630] begin prepare compact";
        Status prepare_ret = index_change_compact->prepare_compact();
        if (!prepare_ret) {
            LOG(INFO) << "[log0630] prepare compact failed";
            return prepare_ret;
        }

        // todo: rethinking should retry in BE or FE when meeting error.
        Status ret = index_change_compact->request_global_lock();
        if (!ret.ok()) {
            LOG(INFO) << "[log0630] request global lock failed";
            return ret;
        }

        LOG(INFO) << "[log0630] request global lock succ";
        Status exec_ret = index_change_compact->execute_compact();
        if (!exec_ret.ok()) {
            LOG(INFO) << "[log0630] exec compaction failed";
            return exec_ret;
        }
        LOG(INFO) << "[log0630] exec compaction succ";
    }

    return Status::OK();
}
} // namespace doris