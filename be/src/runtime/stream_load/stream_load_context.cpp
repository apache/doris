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

#include "runtime/stream_load/stream_load_context.h"

namespace doris {

std::string StreamLoadContext::to_json() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // txn id
    writer.Key("TxnId");
    writer.Int64(txn_id);

    // label
    writer.Key("Label");
    writer.String(label.c_str());

    // status
    writer.Key("Status");
    switch (status.code()) {
    case TStatusCode::OK:
        writer.String("Success");
        break;
    case TStatusCode::PUBLISH_TIMEOUT:
        writer.String("Publish Timeout");
        break;
    case TStatusCode::LABEL_ALREADY_EXISTS:
        writer.String("Label Already Exists");
        writer.Key("ExistingJobStatus");
        writer.String(existing_job_status.c_str());
        break;
    default:
        writer.String("Fail");
        break;
    }
    // msg
    writer.Key("Message");
    if (status.ok()) {
        writer.String("OK");
    } else {
        writer.String(status.get_error_msg().c_str());
    }
    // number_load_rows
    writer.Key("NumberTotalRows");
    writer.Int64(number_total_rows);
    writer.Key("NumberLoadedRows");
    writer.Int64(number_loaded_rows);
    writer.Key("NumberFilteredRows");
    writer.Int64(number_filtered_rows);
    writer.Key("NumberUnselectedRows");
    writer.Int64(number_unselected_rows);
    writer.Key("LoadBytes");
    writer.Int64(receive_bytes);
    writer.Key("LoadTimeMs");
    writer.Int64(load_cost_nanos / 1000000);
    writer.Key("BeginTxnTimeMs");
    writer.Int64(begin_txn_cost_nanos / 1000000);
    writer.Key("StreamLoadPutTimeMs");
    writer.Int64(stream_load_put_cost_nanos / 1000000);
    writer.Key("ReadDataTimeMs");
    writer.Int64(read_data_cost_nanos / 1000000);
    writer.Key("WriteDataTimeMs");
    writer.Int(write_data_cost_nanos / 1000000);
    writer.Key("CommitAndPublishTimeMs");
    writer.Int64(commit_and_publish_txn_cost_nanos / 1000000);

    if (!error_url.empty()) {
        writer.Key("ErrorURL");
        writer.String(error_url.c_str());
    }
    writer.EndObject();
    return s.GetString();
}

/*
 * The old mini load result format is as follows:
 * (which defined in src/util/json_util.cpp)
 *
 * {
 *      "status" : "Success"("Fail"),
 *      "msg"    : "xxxx"
 * }
 *
 */
std::string StreamLoadContext::to_json_for_mini_load() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);
    writer.StartObject();

    // status
    bool show_ok = true;
    writer.Key("status");
    switch (status.code()) {
    case TStatusCode::OK:
        writer.String("Success");
        break;
    case TStatusCode::PUBLISH_TIMEOUT:
        // treat PUBLISH_TIMEOUT as OK in mini load
        writer.String("Success");
        break;
    default:
        writer.String("Fail");
        show_ok = false;
        break;
    }
    // msg
    writer.Key("msg");
    if (status.ok() || show_ok) {
        writer.String("OK");
    } else {
        writer.String(status.get_error_msg().c_str());
    }
    writer.EndObject();
    return s.GetString();
}

std::string StreamLoadContext::brief(bool detail) const {
    std::stringstream ss;
    ss << "id=" << id << ", job_id=" << job_id << ", txn_id=" << txn_id << ", label=" << label;
    if (detail) {
        switch (load_src_type) {
        case TLoadSourceType::KAFKA:
            if (kafka_info != nullptr) {
                ss << ", kafka"
                   << ", brokers: " << kafka_info->brokers << ", topic: " << kafka_info->topic
                   << ", partition: ";
                for (auto& entry : kafka_info->begin_offset) {
                    ss << "[" << entry.first << ": " << entry.second << "]";
                }
            }
            break;
        default:
            break;
        }
    }
    return ss.str();
}

} // namespace doris
