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

#pragma once

#include <brpc/controller.h>
#include <gen_cpp/cloud.pb.h>

#include <memory>
#include <string>
#include <string_view>

#include "common/bvars.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

template <class Request>
void begin_rpc(std::string_view func_name, brpc::Controller* ctrl, const Request* req) {
    if constexpr (std::is_same_v<Request, CreateRowsetRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side();
    } else if constexpr (std::is_same_v<Request, CreateTabletsRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side();
    } else if constexpr (std::is_same_v<Request, UpdateDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " tablet_id=" << req->tablet_id() << " lock_id=" << req->lock_id()
                  << " initiator=" << req->initiator()
                  << " delete_bitmap_size=" << req->segment_delete_bitmaps_size();
    } else if constexpr (std::is_same_v<Request, GetDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " tablet_id=" << req->tablet_id() << " rowset_size=" << req->rowset_ids_size();
    } else if constexpr (std::is_same_v<Request, GetTabletStatsRequest>) {
        VLOG_DEBUG << "begin " << func_name << " from " << ctrl->remote_side()
                   << " tablet size: " << req->tablet_idx().size();
    } else if constexpr (std::is_same_v<Request, GetVersionRequest> ||
                         std::is_same_v<Request, GetRowsetRequest> ||
                         std::is_same_v<Request, GetTabletRequest>) {
        VLOG_DEBUG << "begin " << func_name << " from " << ctrl->remote_side()
                   << " request=" << req->ShortDebugString();
    } else {
        LOG(INFO) << "begin " << func_name << " from " << ctrl->remote_side()
                  << " request=" << req->ShortDebugString();
    }
}

template <class Response>
void finish_rpc(std::string_view func_name, brpc::Controller* ctrl, Response* res) {
    if constexpr (std::is_same_v<Response, CommitTxnResponse>) {
        if (res->status().code() != MetaServiceCode::OK) {
            res->clear_table_ids();
            res->clear_partition_ids();
            res->clear_versions();
        }
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetRowsetResponse>) {
        if (res->status().code() != MetaServiceCode::OK) {
            res->clear_rowset_meta();
        }
        VLOG_DEBUG << "finish " << func_name << " from " << ctrl->remote_side()
                   << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetTabletStatsResponse>) {
        VLOG_DEBUG << "finish " << func_name << " from " << ctrl->remote_side()
                   << " status=" << res->status().ShortDebugString()
                   << " tablet size: " << res->tablet_stats().size();
    } else if constexpr (std::is_same_v<Response, GetVersionResponse> ||
                         std::is_same_v<Response, GetTabletResponse>) {
        VLOG_DEBUG << "finish " << func_name << " from " << ctrl->remote_side()
                   << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetDeleteBitmapResponse>) {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " status=" << res->status().ShortDebugString()
                  << " delete_bitmap_size=" << res->segment_delete_bitmaps_size();

    } else {
        LOG(INFO) << "finish " << func_name << " from " << ctrl->remote_side()
                  << " response=" << res->ShortDebugString();
    }
}

enum ErrCategory { CREATE, READ, COMMIT };

template <ErrCategory category>
inline MetaServiceCode cast_as(TxnErrorCode code) {
    switch (code) {
    case TxnErrorCode::TXN_OK:
        return MetaServiceCode::OK;
    case TxnErrorCode::TXN_CONFLICT:
        return MetaServiceCode::KV_TXN_CONFLICT;
    case TxnErrorCode::TXN_TOO_OLD:
        return MetaServiceCode::KV_TXN_TOO_OLD;
    case TxnErrorCode::TXN_RETRYABLE_NOT_COMMITTED:
        if (config::enable_txn_store_retry) {
            if constexpr (category == ErrCategory::READ) {
                return MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE;
            } else {
                return MetaServiceCode::KV_TXN_STORE_COMMIT_RETRYABLE;
            }
        }
        [[fallthrough]];
    case TxnErrorCode::TXN_KEY_NOT_FOUND:
    case TxnErrorCode::TXN_MAYBE_COMMITTED:
    case TxnErrorCode::TXN_TIMEOUT:
    case TxnErrorCode::TXN_INVALID_ARGUMENT:
    case TxnErrorCode::TXN_UNIDENTIFIED_ERROR:
    case TxnErrorCode::TXN_KEY_TOO_LARGE:
    case TxnErrorCode::TXN_VALUE_TOO_LARGE:
    case TxnErrorCode::TXN_BYTES_TOO_LARGE:
        if constexpr (category == ErrCategory::READ) {
            return MetaServiceCode::KV_TXN_GET_ERR;
        } else if constexpr (category == ErrCategory::CREATE) {
            return MetaServiceCode::KV_TXN_CREATE_ERR;
        } else {
            return MetaServiceCode::KV_TXN_COMMIT_ERR;
        }
    default:
        return MetaServiceCode::UNDEFINED_ERR;
    }
}

#define RPC_PREPROCESS(func_name)                                                        \
    StopWatch sw;                                                                        \
    auto ctrl = static_cast<brpc::Controller*>(controller);                              \
    begin_rpc(#func_name, ctrl, request);                                                \
    brpc::ClosureGuard closure_guard(done);                                              \
    [[maybe_unused]] std::stringstream ss;                                               \
    [[maybe_unused]] MetaServiceCode code = MetaServiceCode::OK;                         \
    [[maybe_unused]] std::string msg;                                                    \
    [[maybe_unused]] std::string instance_id;                                            \
    [[maybe_unused]] bool drop_request = false;                                          \
    std::unique_ptr<int, std::function<void(int*)>> defer_status((int*)0x01, [&](int*) { \
        response->mutable_status()->set_code(code);                                      \
        response->mutable_status()->set_msg(msg);                                        \
        finish_rpc(#func_name, ctrl, response);                                          \
        closure_guard.reset(nullptr);                                                    \
        if (config::use_detailed_metrics && !instance_id.empty() && !drop_request) {     \
            g_bvar_ms_##func_name.put(instance_id, sw.elapsed_us());                     \
        }                                                                                \
    });

#define RPC_RATE_LIMIT(func_name)                                                            \
    if (config::enable_rate_limit && config::use_detailed_metrics && !instance_id.empty()) { \
        auto rate_limiter = rate_limiter_->get_rpc_rate_limiter(#func_name);                 \
        assert(rate_limiter != nullptr);                                                     \
        std::function<int()> get_bvar_qps = [&] {                                            \
            return g_bvar_ms_##func_name.get(instance_id)->qps();                            \
        };                                                                                   \
        if (!rate_limiter->get_qps_token(instance_id, get_bvar_qps)) {                       \
            drop_request = true;                                                             \
            code = MetaServiceCode::MAX_QPS_LIMIT;                                           \
            msg = "reach max qps limit";                                                     \
            return;                                                                          \
        }                                                                                    \
    }

// FIXME(gavin): should it be a member function of ResourceManager?
std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                            const std::string& cloud_unique_id);

int decrypt_instance_info(InstanceInfoPB& instance, const std::string& instance_id,
                          MetaServiceCode& code, std::string& msg,
                          std::shared_ptr<Transaction>& txn);

/**
 * Notifies other metaservice to refresh instance
 */
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id);

void get_tablet_idx(MetaServiceCode& code, std::string& msg, Transaction* txn,
                    const std::string& instance_id, int64_t tablet_id, TabletIndexPB& tablet_idx);

bool is_dropped_tablet(Transaction* txn, const std::string& instance_id, int64_t index_id,
                       int64_t partition_id);

} // namespace doris::cloud
