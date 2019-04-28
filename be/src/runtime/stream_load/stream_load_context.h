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

#include <future>
#include <sstream>
#include <rapidjson/prettywriter.h>

#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/FrontendService_types.h"

#include "common/status.h"
#include "common/logging.h"
#include "common/utils.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "service/backend_options.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

// kafka related info
class KafkaLoadInfo {
public:
    KafkaLoadInfo(const TKafkaLoadInfo& t_info):
        brokers(t_info.brokers),
        topic(t_info.topic),
        begin_offset(t_info.partition_begin_offset) {

        if (t_info.__isset.max_interval_s) { max_interval_s = t_info.max_interval_s; }
        if (t_info.__isset.max_batch_rows) { max_batch_rows = t_info.max_batch_rows; }
        if (t_info.__isset.max_batch_size) { max_batch_size = t_info.max_batch_size; }

        std::stringstream ss;
        ss << BackendOptions::get_localhost() << "_";
        client_id = ss.str() + UniqueId().to_string();
        group_id = ss.str() + UniqueId().to_string();
    }

public:
    std::string brokers;
    std::string topic;
    std::string group_id;
    std::string client_id;

    // the following members control the max progress of a consuming
    // process. if any of them reach, the consuming will finish.
    int64_t max_interval_s = 5;
    int64_t max_batch_rows = 1024;
    int64_t max_batch_size = 100 * 1024 * 1024; // 100MB

    // partition -> begin offset, inclusive.
    std::map<int32_t, int64_t> begin_offset;
    // partiton -> commit offset, inclusive.
    std::map<int32_t, int64_t> cmt_offset;
};

class MessageBodySink;

class StreamLoadContext {
public:
    StreamLoadContext(ExecEnv* exec_env) :
        _exec_env(exec_env),
        _refs(0) {
        start_nanos = MonotonicNanos();
    }

    ~StreamLoadContext() {
        if (need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(this);
            need_rollback = false;
        }

        _exec_env->load_stream_mgr()->remove(id);

        if (kafka_info != nullptr) {
            delete kafka_info;
        }
    }

    void rollback();

    std::string to_json() const;

    // return the brief info of this context.
    // also print the load source info if detail is set to true
    std::string brief(bool detail = false) const;

    void ref() { _refs.fetch_add(1); }
    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

public:
    // load type, eg: ROUTINE LOAD/MANUL LOAD
    TLoadType::type load_type;
    // load data source: eg: KAFKA/RAW
    TLoadSourceType::type load_src_type;

    // the job this stream load task belongs to,
    // set to -1 if there is no job
    int64_t job_id = -1;

    // id for each load
    UniqueId id;

    std::string db;
    std::string table;
    std::string label;

    std::string user_ip;

    AuthInfo auth;

    // only used to check if we receive whole body
    size_t body_bytes = 0;
    size_t receive_bytes = 0;

    int64_t txn_id = -1;

    bool need_rollback = false;
    // when use_streaming is true, we use stream_pipe to send source data,
    // otherwise we save source data to file first, then process it.
    bool use_streaming = false;
    TFileFormatType::type format = TFileFormatType::FORMAT_CSV_PLAIN;

    std::shared_ptr<MessageBodySink> body_sink;

    TStreamLoadPutResult put_result;
    double max_filter_ratio = 0.0;
    std::vector<TTabletCommitInfo> commit_infos;

    std::promise<Status> promise;
    std::future<Status> future = promise.get_future();

    Status status;

    int64_t number_loaded_rows = 0;
    int64_t number_filtered_rows = 0;
    int64_t loaded_bytes = 0;
    int64_t start_nanos = 0;
    int64_t load_cost_nanos = 0;
    std::string error_url;

    KafkaLoadInfo* kafka_info = nullptr;

private:
    ExecEnv* _exec_env;
    std::atomic<int> _refs;
};

} // end namespace
