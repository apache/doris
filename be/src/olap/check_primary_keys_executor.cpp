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

#include "olap/check_primary_keys_executor.h"

#include "common/config.h"
#include "common/logging.h"
#include "olap/tablet.h"

namespace doris {
using namespace ErrorCode;

Status CheckPrimaryKeysToken::submit(Tablet* tablet, const PartialUpdateReadPlan* read_plan,
                                     const std::map<RowsetId, RowsetSharedPtr>* rsid_to_rowset,
                                     std::unordered_map<uint32_t, std::string>* pk_entries,
                                     bool with_seq_col) {
    {
        std::shared_lock rlock(_mutex);
        RETURN_IF_ERROR(_status);
    }
    return _thread_token->submit_func([=, this]() {
        auto st = tablet->check_primary_keys_consistency(read_plan, rsid_to_rowset, pk_entries,
                                                         with_seq_col);
        if (!st.ok()) {
            std::lock_guard wlock(_mutex);
            if (_status.ok()) {
                _status = st;
            }
        }
    });
}

Status CheckPrimaryKeysToken::submit(Tablet* tablet, const PartialUpdateReadPlan* read_plan,
                                     const std::map<RowsetId, RowsetSharedPtr>* rsid_to_rowset,
                                     segment_v2::SegmentWriter* segment_writer,
                                     std::vector<vectorized::IOlapColumnDataAccessor*>* key_columns,
                                     uint32_t row_pos) {
    {
        std::shared_lock rlock(_mutex);
        RETURN_IF_ERROR(_status);
    }
    return _thread_token->submit_func([=, this]() {
        auto st = tablet->check_primary_keys_consistency(read_plan, rsid_to_rowset, segment_writer,
                                                         key_columns, row_pos);
        if (!st.ok()) {
            std::lock_guard wlock(_mutex);
            if (_status.ok()) {
                _status = st;
            }
        }
    });
}

Status CheckPrimaryKeysToken::wait() {
    _thread_token->wait();
    return _status;
}

void CheckPrimaryKeysExecutor::init() {
    static_cast<void>(ThreadPoolBuilder("TabletCheckPrimaryKeysThreadPool")
                              .set_min_threads(1)
                              .set_max_threads(config::check_primary_keys_max_thread)
                              .build(&_thread_pool));
}

std::unique_ptr<CheckPrimaryKeysToken> CheckPrimaryKeysExecutor::create_token() {
    return std::make_unique<CheckPrimaryKeysToken>(
            _thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT));
}

} // namespace doris
