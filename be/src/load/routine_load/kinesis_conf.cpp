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

#include "load/routine_load/kinesis_conf.h"

#include <glog/logging.h>

namespace doris {

KinesisConf::ConfResult KinesisConf::set(const std::string& name, const std::string& value,
                                          std::string& errstr) {
    // Determine which API(s) this parameter belongs to based on its semantic meaning
    // All parameters come in as simple names (e.g., "max_records", "stream_arn")
    // after "aws.kinesis." prefix is removed in data_consumer.cpp

    if (name == "max_records") {
        // GetRecords API parameter
        int max_records;
        if (!parse_int(value, max_records, errstr)) {
            return CONF_INVALID;
        }
        if (max_records < 1 || max_records > 10000) {
            errstr = "max_records must be between 1 and 10000";
            return CONF_INVALID;
        }
        _get_records_params[name] = value;
        return CONF_OK;
    } else if (name == "stream_arn") {
        // Used by all three APIs
        _get_records_params[name] = value;
        _get_shard_iterator_params[name] = value;
        _list_shards_params[name] = value;
        return CONF_OK;
    } else if (name == "timestamp") {
        // GetShardIterator API parameter (for AT_TIMESTAMP iterator type)
        long timestamp;
        if (!parse_long(value, timestamp, errstr)) {
            return CONF_INVALID;
        }
        _get_shard_iterator_params[name] = value;
        return CONF_OK;
    } else if (name == "max_results") {
        // ListShards API parameter
        int max_results;
        if (!parse_int(value, max_results, errstr)) {
            return CONF_INVALID;
        }
        if (max_results < 1 || max_results > 10000) {
            errstr = "max_results must be between 1 and 10000";
            return CONF_INVALID;
        }
        _list_shards_params[name] = value;
        return CONF_OK;
    }

    VLOG_NOTICE << "Unknown Kinesis configuration: " << name;
    return CONF_UNKNOWN;
}

Status KinesisConf::apply_to_get_records_request(
        Aws::Kinesis::Model::GetRecordsRequest& request,
        const std::string& shard_iterator) const {
    request.SetShardIterator(shard_iterator);

    auto it = _get_records_params.find("max_records");
    if (it != _get_records_params.end()) {
        try {
            request.SetLimit(std::stoi(it->second));
        } catch (const std::exception&) {
            return Status::InternalError("Failed to apply get_records.max_records: {}",
                                         it->second);
        }
    }

    it = _get_records_params.find("stream_arn");
    if (it != _get_records_params.end() && !it->second.empty()) {
        request.SetStreamARN(it->second);
    }

    return Status::OK();
}

Status KinesisConf::apply_to_get_shard_iterator_request(
        Aws::Kinesis::Model::GetShardIteratorRequest& request, const std::string& stream_name,
        const std::string& shard_id, const std::string& sequence_number) const {
    request.SetStreamName(stream_name);
    request.SetShardId(shard_id);

    if (sequence_number.empty() || sequence_number == "TRIM_HORIZON" || sequence_number == "-2") {
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
    } else if (sequence_number == "LATEST" || sequence_number == "-1") {
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);
    } else {
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
        request.SetStartingSequenceNumber(sequence_number);
    }

    auto it = _get_shard_iterator_params.find("stream_arn");
    if (it != _get_shard_iterator_params.end() && !it->second.empty()) {
        request.SetStreamARN(it->second);
    }

    it = _get_shard_iterator_params.find("timestamp");
    if (it != _get_shard_iterator_params.end()) {
        try {
            request.SetTimestamp(Aws::Utils::DateTime(std::stol(it->second)));
        } catch (const std::exception&) {
            return Status::InternalError("Failed to apply get_shard_iterator.timestamp: {}",
                                         it->second);
        }
    }

    return Status::OK();
}

Status KinesisConf::apply_to_list_shards_request(
        Aws::Kinesis::Model::ListShardsRequest& request,
        const std::string& stream_name) const {
    request.SetStreamName(stream_name);

    auto it = _list_shards_params.find("stream_arn");
    if (it != _list_shards_params.end() && !it->second.empty()) {
        request.SetStreamARN(it->second);
    }

    it = _list_shards_params.find("max_results");
    if (it != _list_shards_params.end()) {
        try {
            request.SetMaxResults(std::stoi(it->second));
        } catch (const std::exception&) {
            return Status::InternalError("Failed to apply list_shards.max_results: {}",
                                         it->second);
        }
    }

    return Status::OK();
}

bool KinesisConf::parse_int(const std::string& value, int& result, std::string& errstr) const {
    try {
        result = std::stoi(value);
        return true;
    } catch (const std::exception&) {
        errstr = "Invalid integer value: " + value;
        return false;
    }
}

bool KinesisConf::parse_long(const std::string& value, long& result, std::string& errstr) const {
    try {
        result = std::stol(value);
        return true;
    } catch (const std::exception&) {
        errstr = "Invalid long value: " + value;
        return false;
    }
}

} // namespace doris
