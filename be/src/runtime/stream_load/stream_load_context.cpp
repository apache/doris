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
    if (!error_url.empty()) {
        writer.Key("ErrorURL");
        writer.String(error_url.c_str());
    }
    writer.EndObject();
    return s.GetString();
}

std::string StreamLoadContext::brief(bool detail) const {
    std::stringstream ss;
    ss << "id=" << id << ", job id=" << job_id << ", txn id=" << txn_id << ", label=" << label;
    if (detail) {
        switch(load_src_type) {
            case TLoadSourceType::KAFKA:
                if (kafka_info != nullptr) {
                    ss << ", kafka"
                       << ", brokers: " << kafka_info->brokers
                       << ", topic: " << kafka_info->topic
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

} // end namespace
