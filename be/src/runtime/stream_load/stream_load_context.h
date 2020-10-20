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
#include "util/string_util.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

// kafka related info
class KafkaLoadInfo {
public:
    KafkaLoadInfo(const TKafkaLoadInfo& t_info):
        brokers(t_info.brokers),
        topic(t_info.topic),
        begin_offset(t_info.partition_begin_offset),
        properties(t_info.properties) {

        for (auto& p : t_info.partition_begin_offset) {
            cmt_offset[p.first] = p.second -1;
        }
    }

    void reset_offset() {
        // reset the commit offset
        for (auto& p : begin_offset) {
            cmt_offset[p.first] = p.second -1;
        }
    }

public:
    std::string brokers;
    std::string topic;

    // the following members control the max progress of a consuming
    // process. if any of them reach, the consuming will finish.
    int64_t max_interval_s = 5;
    int64_t max_batch_rows = 1024;
    int64_t max_batch_size = 100 * 1024 * 1024; // 100MB

    // partition -> begin offset, inclusive.
    std::map<int32_t, int64_t> begin_offset;
    // partition -> commit offset, inclusive.
    std::map<int32_t, int64_t> cmt_offset;
    //custom kafka property key -> value
    std::map<std::string, std::string> properties;
};

class MessageBodySink;

class StreamLoadContext {
public:
    StreamLoadContext(ExecEnv* exec_env) :
        id(UniqueId::gen_uid()),
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
    }

    std::string to_json() const;
    // the old mini load result format is not same as stream load.
    // add this function for compatible with old mini load result format.
    std::string to_json_for_mini_load() const;

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
    // optional
    std::string sub_label;
    double max_filter_ratio = 0.0;
    int32_t timeout_second = -1;
    AuthInfo auth;

    // the following members control the max progress of a consuming
    // process. if any of them reach, the consuming will finish.
    int64_t max_interval_s = 5;
    int64_t max_batch_rows = 100000;
    int64_t max_batch_size = 100 * 1024 * 1024; // 100MB

    // for parse json-data
    std::string data_format = "";
    std::string jsonpath_file = "";
    std::string jsonpath = "";

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

    std::vector<TTabletCommitInfo> commit_infos;

    std::promise<Status> promise;
    std::future<Status> future = promise.get_future();

    Status status;

    int64_t number_total_rows = 0;
    int64_t number_loaded_rows = 0;
    int64_t number_filtered_rows = 0;
    int64_t number_unselected_rows = 0;
    int64_t loaded_bytes = 0;
    int64_t start_nanos = 0;
    int64_t start_write_data_nanos = 0;
    int64_t load_cost_nanos = 0;
    int64_t begin_txn_cost_nanos = 0;
    int64_t stream_load_put_cost_nanos = 0;
    int64_t commit_and_publish_txn_cost_nanos = 0;
    int64_t read_data_cost_nanos = 0;
    int64_t write_data_cost_nanos = 0;

    std::string error_url = "";
    // if label already be used, set existing job's status here
    // should be RUNNING or FINISHED
    std::string existing_job_status = "";

    std::unique_ptr<KafkaLoadInfo> kafka_info;

    // consumer_id is used for data consumer cache key.
    // to identified a specified data consumer.
    int64_t consumer_id;

public:
    ExecEnv* exec_env() { return _exec_env; }

private:
    ExecEnv* _exec_env;
    std::atomic<int> _refs;
};

} // end namespace
