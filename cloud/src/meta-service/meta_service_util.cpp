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

#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <ios>
#include <limits>
#include <memory>
#include <numeric>
#include <ostream>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/stats.h"
#include "common/stopwatch.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/delete_bitmap_lock_white_list.h"
#include "meta-service/doris_txn.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-store/blob_message.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "meta_service.h"
#include "rate-limiter/rate_limiter.h"

using namespace std::chrono;

namespace doris::cloud {

void MetaServiceImpl::update_table_version(Transaction* txn, std::string_view instance_id,
                                           int64_t db_id, int64_t table_id) {
    LOG_INFO("update table version").tag("db_id", db_id).tag("table_id", table_id);

    std::string ver_key = table_version_key({instance_id, db_id, table_id});
    txn->atomic_add(ver_key, 1);
    if (is_version_write_enabled(instance_id)) {
        std::string table_version_key = versioned::table_version_key({instance_id, table_id});
        versioned_put(txn, table_version_key, "");
    }
}

bool MetaServiceImpl::is_version_read_enabled(std::string_view instance_id) const {
    return resource_mgr_->is_version_read_enabled(instance_id);
}

bool MetaServiceImpl::is_version_write_enabled(std::string_view instance_id) const {
    return resource_mgr_->is_version_write_enabled(instance_id);
}

} // namespace doris::cloud
