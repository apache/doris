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

#include <gen_cpp/BackendService_types.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <new>
#include <sstream>

#include "common/logging.h"

namespace doris {
using namespace ErrorCode;

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

    // comment
    writer.Key("Comment");
    writer.String(load_comment.c_str());

    if (!group_commit) {
        writer.Key("TwoPhaseCommit");
        std::string need_two_phase_commit = two_phase_commit ? "true" : "false";
        writer.String(need_two_phase_commit.c_str());
    } else {
        writer.Key("GroupCommit");
        writer.Bool(true);
    }

    // status
    writer.Key("Status");
    switch (status.code()) {
    case OK:
        writer.String("Success");
        break;
    case PUBLISH_TIMEOUT:
        writer.String("Publish Timeout");
        break;
    case LABEL_ALREADY_EXISTS:
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
        writer.String(status.to_string().c_str());
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
    writer.Int64(load_cost_millis);
    if (!group_commit) {
        writer.Key("BeginTxnTimeMs");
        writer.Int64(begin_txn_cost_nanos / 1000000);
    }
    writer.Key("StreamLoadPutTimeMs");
    writer.Int64(stream_load_put_cost_nanos / 1000000);
    writer.Key("ReadDataTimeMs");
    writer.Int64(read_data_cost_nanos / 1000000);
    writer.Key("WriteDataTimeMs");
    writer.Int(write_data_cost_nanos / 1000000);
    if (!group_commit) {
        writer.Key("CommitAndPublishTimeMs");
        writer.Int64(commit_and_publish_txn_cost_nanos / 1000000);
    }

    if (!error_url.empty()) {
        writer.Key("ErrorURL");
        writer.String(error_url.c_str());
    }
    writer.EndObject();
    return s.GetString();
}

std::string StreamLoadContext::prepare_stream_load_record(const std::string& stream_load_record) {
    rapidjson::Document document;
    if (document.Parse(stream_load_record.data(), stream_load_record.length()).HasParseError()) {
        LOG(WARNING) << "prepare stream load record failed. failed to parse json returned to "
                        "client. label="
                     << label;
        return "";
    }
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

    rapidjson::Value cluster_value(rapidjson::kStringType);
    cluster_value.SetString(auth.cluster.c_str(), auth.cluster.size());
    if (!cluster_value.IsNull()) {
        document.AddMember("cluster", cluster_value, allocator);
    }

    rapidjson::Value db_value(rapidjson::kStringType);
    db_value.SetString(db.c_str(), db.size());
    if (!db_value.IsNull()) {
        document.AddMember("Db", db_value, allocator);
    }

    rapidjson::Value table_value(rapidjson::kStringType);
    table_value.SetString(table.c_str(), table.size());
    if (!table_value.IsNull()) {
        document.AddMember("Table", table_value, allocator);
    }

    rapidjson::Value user_value(rapidjson::kStringType);
    user_value.SetString(auth.user.c_str(), auth.user.size());
    if (!user_value.IsNull()) {
        document.AddMember("User", user_value, allocator);
    }

    rapidjson::Value client_ip_value(rapidjson::kStringType);
    client_ip_value.SetString(auth.user_ip.c_str(), auth.user_ip.size());
    if (!client_ip_value.IsNull()) {
        document.AddMember("ClientIp", client_ip_value, allocator);
    }

    rapidjson::Value comment_value(rapidjson::kStringType);
    comment_value.SetString(load_comment.c_str(), load_comment.size());
    if (!comment_value.IsNull()) {
        document.AddMember("Comment", comment_value, allocator);
    }

    document.AddMember("StartTime", start_millis, allocator);
    document.AddMember("FinishTime", start_millis + load_cost_millis, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    return buffer.GetString();
}

void StreamLoadContext::parse_stream_load_record(const std::string& stream_load_record,
                                                 TStreamLoadRecord& stream_load_item) {
    rapidjson::Document document;
    std::stringstream ss;
    if (document.Parse(stream_load_record.data(), stream_load_record.length()).HasParseError()) {
        LOG(WARNING) << "failed to parse json from rocksdb.";
        return;
    }

    if (document.HasMember("Label")) {
        const rapidjson::Value& label = document["Label"];
        stream_load_item.__set_label(label.GetString());
        ss << "Label: " << label.GetString();
    }

    if (document.HasMember("Db")) {
        const rapidjson::Value& db = document["Db"];
        stream_load_item.__set_db(db.GetString());
        ss << ", Db: " << db.GetString();
    }

    if (document.HasMember("Table")) {
        const rapidjson::Value& table = document["Table"];
        stream_load_item.__set_tbl(table.GetString());
        ss << ", Table: " << table.GetString();
    }

    if (document.HasMember("User")) {
        const rapidjson::Value& user = document["User"];
        stream_load_item.__set_user(user.GetString());
        ss << ", User: " << user.GetString();
    }

    if (document.HasMember("ClientIp")) {
        const rapidjson::Value& client_ip = document["ClientIp"];
        stream_load_item.__set_user_ip(client_ip.GetString());
        ss << ", ClientIp: " << client_ip.GetString();
    }

    if (document.HasMember("Status")) {
        const rapidjson::Value& status = document["Status"];
        stream_load_item.__set_status(status.GetString());
        ss << ", Status: " << status.GetString();
    }

    if (document.HasMember("Message")) {
        const rapidjson::Value& message = document["Message"];
        stream_load_item.__set_message(message.GetString());
        ss << ", Message: " << message.GetString();
    }

    if (document.HasMember("ErrorURL")) {
        const rapidjson::Value& error_url = document["ErrorURL"];
        stream_load_item.__set_url(error_url.GetString());
        ss << ", ErrorURL: " << error_url.GetString();
    } else {
        stream_load_item.__set_url("N/A");
        ss << ", ErrorURL: N/A";
    }

    if (document.HasMember("NumberTotalRows")) {
        const rapidjson::Value& total_rows = document["NumberTotalRows"];
        stream_load_item.__set_total_rows(total_rows.GetInt64());
        ss << ", NumberTotalRows: " << total_rows.GetInt64();
    }

    if (document.HasMember("NumberLoadedRows")) {
        const rapidjson::Value& loaded_rows = document["NumberLoadedRows"];
        stream_load_item.__set_loaded_rows(loaded_rows.GetInt64());
        ss << ", NumberLoadedRows: " << loaded_rows.GetInt64();
    }

    if (document.HasMember("NumberFilteredRows")) {
        const rapidjson::Value& filtered_rows = document["NumberFilteredRows"];
        stream_load_item.__set_filtered_rows(filtered_rows.GetInt64());
        ss << ", NumberFilteredRows: " << filtered_rows.GetInt64();
    }

    if (document.HasMember("NumberUnselectedRows")) {
        const rapidjson::Value& unselected_rows = document["NumberUnselectedRows"];
        stream_load_item.__set_unselected_rows(unselected_rows.GetInt64());
        ss << ", NumberUnselectedRows: " << unselected_rows.GetInt64();
    }

    if (document.HasMember("LoadBytes")) {
        const rapidjson::Value& load_bytes = document["LoadBytes"];
        stream_load_item.__set_load_bytes(load_bytes.GetInt64());
        ss << ", LoadBytes: " << load_bytes.GetInt64();
    }

    if (document.HasMember("StartTime")) {
        const rapidjson::Value& start_time = document["StartTime"];
        stream_load_item.__set_start_time(start_time.GetInt64());
        ss << ", StartTime: " << start_time.GetInt64();
    }

    if (document.HasMember("FinishTime")) {
        const rapidjson::Value& finish_time = document["FinishTime"];
        stream_load_item.__set_finish_time(finish_time.GetInt64());
        ss << ", FinishTime: " << finish_time.GetInt64();
    }

    if (document.HasMember("Comment")) {
        const rapidjson::Value& comment_value = document["Comment"];
        stream_load_item.__set_comment(comment_value.GetString());
        ss << ", Comment: " << comment_value.GetString();
    }

    VLOG(1) << "parse json from rocksdb. " << ss.str();
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
    case OK:
        writer.String("Success");
        break;
    case PUBLISH_TIMEOUT:
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
        writer.String(status.to_string().c_str());
    }
    writer.EndObject();
    return s.GetString();
}

std::string StreamLoadContext::brief(bool detail) const {
    std::stringstream ss;
    ss << "id=" << id << ", job_id=" << job_id << ", txn_id=" << txn_id << ", label=" << label
       << ", elapse(s)=" << (UnixMillis() - start_millis) / 1000;
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
