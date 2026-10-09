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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioner/external/external_partition_function_factory.h"

#include <charconv>
#include <string_view>

#include "common/status.h"
#include "exec/partitioner/external/paimon_fixed_bucket_partition_function.h"

namespace doris {
namespace {

Status parse_int32(std::string_view value, int32_t* const result) {
    auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), *result);
    if (error != std::errc() || ptr != value.data() + value.size()) {
        return Status::InvalidArgument("Invalid integer partition function option '{}'", value);
    }
    return Status::OK();
}

Status parse_indexes(std::string_view value, std::vector<int32_t>* const indexes) {
    while (!value.empty()) {
        size_t separator = value.find(',');
        std::string_view item = value.substr(0, separator);
        int32_t index = 0;
        RETURN_IF_ERROR(parse_int32(item, &index));
        indexes->push_back(index);
        if (separator == std::string_view::npos) {
            break;
        }
        value.remove_prefix(separator + 1);
    }
    return Status::OK();
}

Status create_paimon_fixed_bucket_function(
        const TExternalTableSinkHashPartitionInfo& partition_info,
        PartitionerBase::HashValType logical_partition_count,
        const std::vector<TExpr>& partition_exprs,
        std::unique_ptr<PartitionFunction>* const partition_function) {
    if (!partition_info.__isset.partition_function_options) {
        return Status::InvalidArgument("Paimon fixed-bucket partition function requires options");
    }
    const auto& options = partition_info.partition_function_options;
    if (options.size() != 3 || !options.contains("num_buckets") ||
        !options.contains("partition_field_indexes") || !options.contains("bucket_field_indexes")) {
        return Status::InvalidArgument(
                "Paimon fixed-bucket partition function requires num_buckets, "
                "partition_field_indexes, and bucket_field_indexes options");
    }
    int32_t num_buckets = 0;
    std::vector<int32_t> partition_indexes;
    std::vector<int32_t> bucket_indexes;
    RETURN_IF_ERROR(parse_int32(options.at("num_buckets"), &num_buckets));
    RETURN_IF_ERROR(parse_indexes(options.at("partition_field_indexes"), &partition_indexes));
    RETURN_IF_ERROR(parse_indexes(options.at("bucket_field_indexes"), &bucket_indexes));
    auto function = std::make_unique<PaimonFixedBucketPartitionFunction>(
            logical_partition_count, num_buckets, std::move(partition_indexes),
            std::move(bucket_indexes));
    RETURN_IF_ERROR(function->init(partition_exprs));
    *partition_function = std::move(function);
    return Status::OK();
}

} // namespace

Status create_external_partition_function(
        const TExternalTableSinkHashPartitionInfo& partition_info,
        PartitionerBase::HashValType logical_partition_count, ShuffleHashMethod hash_method,
        const std::vector<TExpr>& partition_exprs,
        std::unique_ptr<PartitionFunction>* const partition_function) {
    if (partition_function == nullptr) {
        return Status::InvalidArgument("External partition function output is null");
    }
    if (partition_info.partition_function == "paimon_fixed_bucket") {
        return create_paimon_fixed_bucket_function(partition_info, logical_partition_count,
                                                   partition_exprs, partition_function);
    }
    if (partition_info.partition_function != "direct_hash") {
        return Status::NotSupported("Unsupported external sink partition function '{}'",
                                    partition_info.partition_function);
    }
    if (partition_info.__isset.partition_function_options &&
        !partition_info.partition_function_options.empty()) {
        return Status::InvalidArgument("Direct hash partition function does not accept options");
    }
    auto function = std::make_unique<HashPartitionFunction>(logical_partition_count, hash_method);
    RETURN_IF_ERROR(function->init(partition_exprs));
    *partition_function = std::move(function);
    return Status::OK();
}

} // namespace doris
