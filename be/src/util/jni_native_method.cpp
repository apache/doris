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

#include "util/jni_native_method.h"

#include <gen_cpp/FrontendService.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <thread>
#include <vector>

#include "common/status.h"
#include "jni.h"
#include "runtime/exec_env.h"
#include "util/client_cache.h"
#include "util/defer_op.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

namespace {

void throw_java_runtime_exception(JNIEnv* env, const std::string& message) {
    jclass exception_cl = env->FindClass("java/lang/IllegalStateException");
    if (exception_cl != nullptr) {
        env->ThrowNew(exception_cl, message.c_str());
        env->DeleteLocalRef(exception_cl);
    }
}

Result<int64_t> request_maxcompute_block_id_from_fe(int64_t txn_id,
                                                    const std::string& write_session_id) {
    if (txn_id <= 0) {
        return ResultError(Status::InvalidArgument(
                "invalid MaxCompute txn_id for block_id allocation: {}", txn_id));
    }
    if (write_session_id.empty()) {
        return ResultError(Status::InvalidArgument(
                "empty MaxCompute write_session_id for block_id allocation"));
    }

    constexpr uint32_t FETCH_BLOCK_ID_MAX_RETRY_TIMES = 3;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;
    for (uint32_t retry_times = 0; retry_times < FETCH_BLOCK_ID_MAX_RETRY_TIMES; retry_times++) {
        TMaxComputeBlockIdRequest request;
        TMaxComputeBlockIdResult result;
        request.__set_txn_id(txn_id);
        request.__set_write_session_id(write_session_id);
        request.__set_length(1);

        Status rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->getMaxComputeBlockIdRange(result, request);
                });

        if (!rpc_status.ok()) {
            LOG(WARNING) << "Failed to allocate MaxCompute block_id, rpc failure, retry_time="
                         << retry_times << ", txn_id=" << txn_id
                         << ", write_session_id=" << write_session_id << ", status=" << rpc_status;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        if (!result.__isset.status) {
            return ResultError(Status::RpcError(
                    "failed to allocate MaxCompute block_id from FE, missing status in response, "
                    "txn_id={}, write_session_id={}",
                    txn_id, write_session_id));
        }

        Status fe_status = Status::create<false>(result.status);
        if (fe_status.is<ErrorCode::NOT_MASTER>()) {
            if (!result.__isset.master_address) {
                return ResultError(Status::RpcError(
                        "failed to allocate MaxCompute block_id from FE, missing master address "
                        "in NOT_MASTER response, txn_id={}, write_session_id={}",
                        txn_id, write_session_id));
            }
            LOG(WARNING) << "Failed to allocate MaxCompute block_id, requested non-master FE@"
                         << master_addr.hostname << ":" << master_addr.port << ", switch to FE@"
                         << result.master_address.hostname << ":" << result.master_address.port
                         << ", retry_time=" << retry_times << ", txn_id=" << txn_id
                         << ", write_session_id=" << write_session_id;
            master_addr = result.master_address;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        if (!fe_status.ok()) {
            LOG(WARNING) << "Failed to allocate MaxCompute block_id, FE returned error, retry_time="
                         << retry_times << ", txn_id=" << txn_id
                         << ", write_session_id=" << write_session_id << ", status=" << fe_status;
            return ResultError(std::move(fe_status));
        }

        if (result.length != 1) {
            return ResultError(Status::RpcError(
                    "failed to allocate MaxCompute block_id from FE, expected length=1 but got "
                    "{}, txn_id={}, write_session_id={}",
                    result.length, txn_id, write_session_id));
        }

        LOG(INFO) << "Allocated MaxCompute block_id from FE@" << master_addr.hostname << ":"
                  << master_addr.port << ", txn_id=" << txn_id
                  << ", write_session_id=" << write_session_id << ", block_id=" << result.start;
        return result.start;
    }

    return ResultError(Status::RpcError(
            "failed to allocate MaxCompute block_id from FE, txn_id={}, write_session_id={}",
            txn_id, write_session_id));
}

} // namespace

jlong JavaNativeMethods::memoryMalloc(JNIEnv* env, jclass clazz, jlong bytes) {
    return reinterpret_cast<long>(malloc(bytes));
}

void JavaNativeMethods::memoryFree(JNIEnv* env, jclass clazz, jlong address) {
    free(reinterpret_cast<void*>(address));
}

jlongArray JavaNativeMethods::memoryMallocBatch(JNIEnv* env, jclass clazz, jintArray sizes) {
    DCHECK(sizes != nullptr);
    jsize n = env->GetArrayLength(sizes);
    DCHECK(n > 0);
    jint* elems = env->GetIntArrayElements(sizes, nullptr);
    if (elems == nullptr) {
        return nullptr;
    }
    DEFER({
        if (elems != nullptr) {
            env->ReleaseIntArrayElements(sizes, elems, JNI_ABORT);
        }
    });

    jlongArray result = env->NewLongArray(n);
    if (result == nullptr) {
        return nullptr;
    }

    std::vector<void*> allocated;
    allocated.reserve(n);

    // sizes are validated on Java side: n > 0 and each size > 0
    bool failed = false;
    for (jsize i = 0; i < n; ++i) {
        auto sz = static_cast<size_t>(elems[i]);
        void* p = malloc(sz);
        if (p == nullptr) {
            failed = true;
            break;
        }
        allocated.push_back(p);
    }

    if (failed) {
        for (void* p : allocated) {
            if (p != nullptr) {
                free(p);
            }
        }
        return nullptr;
    }

    std::vector<jlong> addrs(n);
    for (jsize i = 0; i < n; ++i) {
        addrs[i] = reinterpret_cast<jlong>(allocated[i]);
    }
    env->SetLongArrayRegion(result, 0, n, addrs.data());
    return result;
}

void JavaNativeMethods::memoryFreeBatch(JNIEnv* env, jclass clazz, jlongArray addrs) {
    if (addrs == nullptr) {
        return;
    }
    jsize n = env->GetArrayLength(addrs);
    if (n <= 0) {
        return;
    }
    jlong* elems = env->GetLongArrayElements(addrs, nullptr);
    if (elems == nullptr) {
        return;
    }
    for (jsize i = 0; i < n; ++i) {
        if (elems[i] != 0) {
            free(reinterpret_cast<void*>(elems[i]));
        }
    }
    env->ReleaseLongArrayElements(addrs, elems, JNI_ABORT);
}

jlong JavaNativeMethods::requestMaxComputeBlockId(JNIEnv* env, jclass clazz, jlong txn_id,
                                                  jstring write_session_id) {
    if (write_session_id == nullptr) {
        throw_java_runtime_exception(
                env, "MaxCompute write_session_id is null when requesting block_id");
        return 0;
    }

    const char* write_session_id_chars = env->GetStringUTFChars(write_session_id, nullptr);
    if (write_session_id_chars == nullptr) {
        throw_java_runtime_exception(env, "Failed to read MaxCompute write_session_id from Java");
        return 0;
    }
    std::string write_session_id_str(write_session_id_chars);
    env->ReleaseStringUTFChars(write_session_id, write_session_id_chars);

    auto block_id = request_maxcompute_block_id_from_fe(txn_id, write_session_id_str);
    if (!block_id.has_value()) {
        throw_java_runtime_exception(env, block_id.error().to_string());
        return 0;
    }
    return static_cast<jlong>(block_id.value());
}

} // namespace doris
