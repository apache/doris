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

#include <aws/core/client/ClientConfiguration.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/ListShardsRequest.h>

#include <map>
#include <string>

namespace doris {

/**
 * KinesisConf - Configuration management for AWS Kinesis API parameters
 *
 * Manages less-frequently-used Kinesis API parameters, organized by API type.
 * Parameters are categorized into three maps based on which API they apply to:
 * - GetRecords API parameters (max_records, stream_arn)
 * - GetShardIterator API parameters (stream_arn, timestamp)
 * - ListShards API parameters (stream_arn, max_results)
 *
 * Note: Frequently-used parameters (stream, shards, positions) are stored
 * as explicit members in KinesisDataConsumer for better performance.
 */
class KinesisConf {
public:
    enum ConfResult {
        CONF_OK = 0,
        CONF_INVALID = 1,
        CONF_UNKNOWN = 2
    };

    KinesisConf() = default;
    ~KinesisConf() = default;

    /**
     * Set a configuration property for Kinesis APIs
     * Automatically categorizes parameters by API type:
     *
     * GetRecords API:
     * - get_records.max_records: Max records per call (1-10000)
     * - get_records.stream_arn: StreamARN for enhanced fan-out
     *
     * GetShardIterator API:
     * - get_shard_iterator.stream_arn: StreamARN for enhanced fan-out
     * - get_shard_iterator.timestamp: Timestamp for AT_TIMESTAMP iterator type
     *
     * ListShards API:
     * - list_shards.stream_arn: StreamARN for enhanced fan-out
     * - list_shards.max_results: Max shards per call (1-10000)
     */
    ConfResult set(const std::string& name, const std::string& value, std::string& errstr);

    Status apply_to_get_records_request(Aws::Kinesis::Model::GetRecordsRequest& request,
                                         const std::string& shard_iterator) const;

    Status apply_to_get_shard_iterator_request(
            Aws::Kinesis::Model::GetShardIteratorRequest& request, const std::string& stream_name,
            const std::string& shard_id, const std::string& sequence_number) const;

    Status apply_to_list_shards_request(Aws::Kinesis::Model::ListShardsRequest& request,
                                         const std::string& stream_name) const;

private:
    // Separate parameter maps for each API
    std::map<std::string, std::string> _get_records_params;
    std::map<std::string, std::string> _get_shard_iterator_params;
    std::map<std::string, std::string> _list_shards_params;

    bool parse_int(const std::string& value, int& result, std::string& errstr) const;
    bool parse_long(const std::string& value, long& result, std::string& errstr) const;
};

} // namespace doris
