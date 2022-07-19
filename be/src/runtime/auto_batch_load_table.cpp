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

#include "runtime/auto_batch_load_table.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <memory>
#include <sstream>

#include "client_cache.h"
#include "common/object_pool.h"
#include "olap/storage_engine.h"
#include "olap/wal_reader.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "util/file_utils.h"
#include "util/path_util.h"
#include "util/url_coding.h"

namespace doris {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

class RuntimeProfile;

AutoBatchLoadTable::AutoBatchLoadTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id), _wal_id(UnixMillis()) {
    std::string path = _exec_env->storage_engine()->auto_batch_load_dir();
    path = path_util::join_path_segments(path, std::to_string(_db_id));
    path = path_util::join_path_segments(path, std::to_string(_table_id));
    // create auto batch load table directory if it does not exist
    if (!FileUtils::check_exist(path)) {
        Status st = FileUtils::create_dir(path);
        CHECK(st.ok()) << st.to_string();
    }
    _table_load_dir = path;
}

AutoBatchLoadTable::~AutoBatchLoadTable() {}

Status AutoBatchLoadTable::auto_batch_load(const PAutoBatchLoadRequest* request, std::string& label,
                                           int64_t& txn_id) {
    return Status::OK();
}

bool AutoBatchLoadTable::need_commit() {
    return false;
}

Status AutoBatchLoadTable::commit(int64_t& wal_id, std::string& wal_path) {
    return Status::OK();
}

Status AutoBatchLoadTable::recovery_wal(const int64_t& wal_id, const std::string& wal_path) {
    return Status::OK();
}

} // namespace doris
