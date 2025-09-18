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
#include <openssl/md5.h>

#include <iomanip>
#include <memory>
#include <string>
#include <string_view>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/stats.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {
inline std::string md5(const std::string& str) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5_CTX context;
    MD5_Init(&context);
    MD5_Update(&context, str.c_str(), str.length());
    MD5_Final(digest, &context);

    std::ostringstream ss;
    for (unsigned char i : digest) {
        ss << std::setw(2) << std::setfill('0') << std::hex << (int)i;
    }
    return ss.str();
}

std::string hide_access_key(const std::string& ak);

// is_handle_sk: true for encrypting sk, false for hiding ak
inline void process_ak_sk_pattern(std::string& str, const std::string& pattern, bool is_handle_sk) {
    size_t pos = 0;
    while ((pos = str.find(pattern, pos)) != std::string::npos) {
        size_t colon_pos = str.find(':', pos);
        if (colon_pos == std::string::npos) {
            pos += pattern.length();
            continue;
        }

        size_t quote_pos = str.find('\"', colon_pos);
        if (quote_pos == std::string::npos) {
            pos += pattern.length();
            continue;
        }

        size_t value_start = quote_pos + 1;
        size_t value_end = str.find('\"', value_start);
        if (value_end == std::string::npos) {
            pos = value_start;
            continue;
        }

        std::string key_value = str.substr(value_start, value_end - value_start);

        if (is_handle_sk) {
            key_value = "md5: " + md5(key_value);
        } else {
            key_value = hide_access_key(key_value);
        }

        str.replace(value_start, value_end - value_start, key_value);

        pos = value_end + (key_value.length() - key_value.length());
    }
};

/**
 * Encrypts all "sk" values in the given debug string with MD5 hashes.
 * 
 * Assumptions:
 * - Input string contains one or more occurrences of "sk: " followed by a value in double quotes.
 * - An md5() function exists that takes a std::string and returns its MD5 hash as a string.
 * 
 * @param debug_string Input string containing "sk: " or ""sk": " fields to be encrypted.
 * @return A new string with all "sk" values replaced by their MD5 hashes.
 * 
 * Behavior for "sk: " format:
 * 1. Searches for all occurrences of "sk: " in the input string.
 * 2. For each occurrence, extracts the value between double quotes.
 * 3. Replaces the original value with "md5: " followed by its MD5 hash.
 * 4. Returns the modified string with all "sk" values encrypted.
 */
inline std::string encryt_sk(std::string debug_string) {
    process_ak_sk_pattern(debug_string, "sk: ", true);
    process_ak_sk_pattern(debug_string, "\"sk\"", true);

    return debug_string;
}

inline std::string hide_ak(std::string debug_string) {
    process_ak_sk_pattern(debug_string, "ak: ", false);
    process_ak_sk_pattern(debug_string, "\"ak\"", false);

    return debug_string;
}

template <class Request, class Response>
void begin_rpc(std::string_view func_name, brpc::Controller* ctrl, const Request* req,
               Response* res) {
    res->Clear(); // clear response in case of this is call is a local retry in MS
    if constexpr (std::is_same_v<Request, CreateRowsetRequest>) {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip();
    } else if constexpr (std::is_same_v<Request, CreateTabletsRequest>) {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip();
    } else if constexpr (std::is_same_v<Request, UpdateDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip() << " table_id=" << req->table_id()
                  << " tablet_id=" << req->tablet_id() << " lock_id=" << req->lock_id()
                  << " initiator=" << req->initiator()
                  << " delete_bitmap_size=" << req->segment_delete_bitmaps_size();
    } else if constexpr (std::is_same_v<Request, GetDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " tablet_id=" << req->tablet_id() << " rowset_size=" << req->rowset_ids_size();
    } else if constexpr (std::is_same_v<Request, GetTabletStatsRequest>) {
        VLOG_DEBUG << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                   << " original_client_ip=" << req->request_ip()
                   << " num_tablets: " << req->tablet_idx().size();
    } else if constexpr (std::is_same_v<Request, GetVersionRequest> ||
                         std::is_same_v<Request, GetRowsetRequest> ||
                         std::is_same_v<Request, GetTabletRequest>) {
        VLOG_DEBUG << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                   << " original_client_ip=" << req->request_ip()
                   << " request=" << req->ShortDebugString();
    } else if constexpr (std::is_same_v<Request, RemoveDeleteBitmapRequest>) {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " tablet_id=" << req->tablet_id() << " rowset_size=" << req->rowset_ids_size();
    } else if constexpr (std::is_same_v<Request, GetDeleteBitmapUpdateLockRequest>) {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip() << " table_id=" << req->table_id()
                  << " lock_id=" << req->lock_id() << " initiator=" << req->initiator()
                  << " expiration=" << req->expiration()
                  << " require_compaction_stats=" << req->require_compaction_stats();
    } else if constexpr (std::is_same_v<Request, CreateInstanceRequest> ||
                         std::is_same_v<Request, CreateStageRequest>) {
        std::string debug_string = encryt_sk(req->ShortDebugString());
        debug_string = hide_ak(debug_string);
        TEST_SYNC_POINT_CALLBACK("sk_begin_rpc", &debug_string);
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip() << " request=" << debug_string;
    } else {
        LOG(INFO) << "begin " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " request=" << req->ShortDebugString();
    }
}

template <class Request, class Response>
void finish_rpc(std::string_view func_name, brpc::Controller* ctrl, const Request* req,
                Response* res) {
    if constexpr (std::is_same_v<Response, CommitTxnResponse>) {
        LOG(INFO) << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetRowsetResponse>) {
        LOG_IF(INFO, res->status().code() != MetaServiceCode::OK)
                << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                << " original_client_ip=" << req->request_ip()
                << " request=" << req->ShortDebugString()
                << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetTabletStatsResponse>) {
        LOG_IF(INFO, res->status().code() != MetaServiceCode::OK)
                << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                << " original_client_ip=" << req->request_ip()
                << " request=" << req->ShortDebugString()
                << " status=" << res->status().ShortDebugString()
                << " num_tablets: " << res->tablet_stats().size();
    } else if constexpr (std::is_same_v<Response, GetVersionResponse> ||
                         std::is_same_v<Response, GetTabletResponse>) {
        LOG_IF(INFO, res->status().code() != MetaServiceCode::OK)
                << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                << " request=" << req->ShortDebugString()
                << " original_client_ip=" << req->request_ip()
                << " response=" << res->ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetDeleteBitmapResponse>) {
        LOG(INFO) << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " status=" << res->status().ShortDebugString()
                  << " tablet=" << res->tablet_id()
                  << " delete_bitmap_count=" << res->segment_delete_bitmaps_size();
    } else if constexpr (std::is_same_v<Response, GetDeleteBitmapUpdateLockResponse>) {
        LOG(INFO) << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " status=" << res->status().ShortDebugString();
    } else if constexpr (std::is_same_v<Response, GetObjStoreInfoResponse> ||
                         std::is_same_v<Response, GetStageResponse> ||
                         std::is_same_v<Response, GetInstanceResponse>) {
        std::string debug_string = encryt_sk(res->DebugString());
        debug_string = hide_ak(debug_string);
        TEST_SYNC_POINT_CALLBACK("sk_finish_rpc", &debug_string);
        LOG(INFO) << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip() << " response=" << debug_string;
    } else {
        LOG(INFO) << "finish " << func_name << " remote_caller=" << ctrl->remote_side()
                  << " original_client_ip=" << req->request_ip()
                  << " response=" << res->ShortDebugString();
    }
}

enum class ErrCategory { CREATE, READ, COMMIT };

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
    case TxnErrorCode::TXN_INVALID_DATA:
    case TxnErrorCode::TXN_UNIDENTIFIED_ERROR:
        if constexpr (category == ErrCategory::READ) {
            return MetaServiceCode::KV_TXN_GET_ERR;
        } else if constexpr (category == ErrCategory::CREATE) {
            return MetaServiceCode::KV_TXN_CREATE_ERR;
        } else {
            return MetaServiceCode::KV_TXN_COMMIT_ERR;
        }
        [[fallthrough]];
    case TxnErrorCode::TXN_KEY_TOO_LARGE:
    case TxnErrorCode::TXN_VALUE_TOO_LARGE:
    case TxnErrorCode::TXN_BYTES_TOO_LARGE:
        return MetaServiceCode::INVALID_ARGUMENT;
    default:
        return MetaServiceCode::UNDEFINED_ERR;
    }
}

// don't use these macro it just for defer count, reduce useless variable(some rpc just need one of rw op)
// If we have to write separate code for each RPC, it would be quite troublesome
// After all, adding put, get, and del after the RPC_PREPROCESS macro is simpler than writing a long string of code
#define RPCKVCOUNTHELPER(func_name, op)                                            \
    g_bvar_rpc_kv_##func_name##_##op##_bytes.put({instance_id}, stats.op##_bytes); \
    g_bvar_rpc_kv_##func_name##_##op##_counter.put({instance_id}, stats.op##_counter);

#define RPCKVCOUNT_0(func_name)
#define RPCKVCOUNT_1(func_name, op1) RPCKVCOUNTHELPER(func_name, op1)
#define RPCKVCOUNT_2(func_name, op1, op2) \
    RPCKVCOUNT_1(func_name, op1) RPCKVCOUNTHELPER(func_name, op2)
#define RPCKVCOUNT_3(func_name, op1, op2, op3) \
    RPCKVCOUNT_2(func_name, op1, op2) RPCKVCOUNTHELPER(func_name, op3)
#define GET_RPCKVCOUNT_MACRO(_0, _1, _2, _3, NAME, ...) NAME

// input func_name, count type(get, put, del), make sure the counter is exist
// about defer_count:
// which means that these bvars will only be counted after stats has finished counting.
// why not cancle KVStats, count directly?
// 1. some RPC operations call functions and function reset txn it also need to be counted
// 2. some function such as `scan_tmp_rowset` it used by RPC(commit_txn) and non rpc
// maybe we can add a bool variable to judge weather we need count, but if have more complex situation
// `func1` used by RPC1, RPC2 and RPC3 judge it or just give func1 a pointer
#define RPC_PREPROCESS(func_name, ...)                                                        \
    StopWatch sw;                                                                             \
    auto ctrl = static_cast<brpc::Controller*>(controller);                                   \
    begin_rpc(#func_name, ctrl, request, response);                                           \
    brpc::ClosureGuard closure_guard(done);                                                   \
    [[maybe_unused]] std::stringstream ss;                                                    \
    [[maybe_unused]] MetaServiceCode code = MetaServiceCode::OK;                              \
    [[maybe_unused]] std::unique_ptr<Transaction> txn;                                        \
    [[maybe_unused]] std::string msg;                                                         \
    [[maybe_unused]] std::string instance_id;                                                 \
    [[maybe_unused]] bool drop_request = false;                                               \
    [[maybe_unused]] KVStats stats;                                                           \
    DORIS_CLOUD_DEFER {                                                                       \
        response->mutable_status()->set_code(code);                                           \
        response->mutable_status()->set_msg(msg);                                             \
        finish_rpc(#func_name, ctrl, request, response);                                      \
        closure_guard.reset(nullptr);                                                         \
        if (txn != nullptr) {                                                                 \
            stats.get_bytes += txn->get_bytes();                                              \
            stats.put_bytes += txn->put_bytes();                                              \
            stats.del_bytes += txn->delete_bytes();                                           \
            stats.get_counter += txn->num_get_keys();                                         \
            stats.put_counter += txn->num_put_keys();                                         \
            stats.del_counter += txn->num_del_keys();                                         \
        }                                                                                     \
        if (config::use_detailed_metrics && !instance_id.empty()) {                           \
            if (!drop_request) {                                                              \
                g_bvar_ms_##func_name.put(instance_id, sw.elapsed_us());                      \
            }                                                                                 \
            GET_RPCKVCOUNT_MACRO(_0, ##__VA_ARGS__, RPCKVCOUNT_3, RPCKVCOUNT_2, RPCKVCOUNT_1, \
                                 RPCKVCOUNT_0)                                                \
            (func_name, ##__VA_ARGS__)                                                        \
        }                                                                                     \
    };

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
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                             KVStats* stats);

void get_tablet_idx(MetaServiceCode& code, std::string& msg, Transaction* txn,
                    const std::string& instance_id, int64_t tablet_id, TabletIndexPB& tablet_idx);

bool is_dropped_tablet(Transaction* txn, const std::string& instance_id, int64_t index_id,
                       int64_t partition_id);

std::size_t get_segments_key_bounds_bytes(const doris::RowsetMetaCloudPB& rowset_meta);
} // namespace doris::cloud
