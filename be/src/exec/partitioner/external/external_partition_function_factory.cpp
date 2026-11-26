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
#include <string>
#include <string_view>

#include "common/status.h"
#include "exec/partitioner/external/paimon_fixed_bucket_partition_function.h"
#include "format/transformer/iceberg_partition_function.h"

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
        PartitionerBase::HashValType logical_partition_count, int32_t num_buckets,
        std::vector<int32_t> partition_indexes, std::vector<int32_t> bucket_indexes,
        const std::vector<TExpr>& partition_exprs,
        std::unique_ptr<PartitionFunction>* const partition_function) {
    auto function = std::make_unique<PaimonFixedBucketPartitionFunction>(
            logical_partition_count, num_buckets, std::move(partition_indexes),
            std::move(bucket_indexes));
    RETURN_IF_ERROR(function->init(partition_exprs));
    *partition_function = std::move(function);
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
    RETURN_IF_ERROR(create_paimon_fixed_bucket_function(
            logical_partition_count, num_buckets, std::move(partition_indexes),
            std::move(bucket_indexes), partition_exprs, partition_function));
    return Status::OK();
}

Status parse_transforms(std::string_view value, std::vector<std::string>* const transforms) {
    while (true) {
        size_t separator = value.find(',');
        std::string_view item = value.substr(0, separator);
        if (item.empty()) {
            return Status::InvalidArgument("Empty Iceberg partition transform in '{}'", value);
        }
        transforms->emplace_back(item);
        if (separator == std::string_view::npos) {
            return Status::OK();
        }
        value.remove_prefix(separator + 1);
    }
}

// Routes each row by its Iceberg partition value: the hash covers the transformed source columns,
// so rows of one partition reach one logical partition no matter how the raw values differ.
Status create_iceberg_transform_function(
        const TExternalTableSinkHashPartitionInfo& partition_info,
        PartitionerBase::HashValType logical_partition_count, ShuffleHashMethod hash_method,
        const std::vector<TExpr>& partition_exprs,
        std::unique_ptr<PartitionFunction>* const partition_function) {
    if (!partition_info.__isset.partition_function_options) {
        return Status::InvalidArgument("Iceberg transform partition function requires options");
    }
    const auto& options = partition_info.partition_function_options;
    if (options.size() != 1 || !options.contains("transforms")) {
        return Status::InvalidArgument(
                "Iceberg transform partition function requires exactly the transforms option");
    }
    std::vector<std::string> transforms;
    RETURN_IF_ERROR(parse_transforms(options.at("transforms"), &transforms));
    if (transforms.size() != partition_exprs.size()) {
        return Status::InvalidArgument(
                "Iceberg partition transform count {} does not match partition expression count {}",
                transforms.size(), partition_exprs.size());
    }
    std::vector<TIcebergPartitionField> fields;
    fields.reserve(partition_exprs.size());
    for (size_t index = 0; index < partition_exprs.size(); ++index) {
        TIcebergPartitionField field;
        field.__set_transform(transforms[index]);
        field.__set_source_expr(partition_exprs[index]);
        fields.emplace_back(std::move(field));
    }
    auto function = std::make_unique<IcebergInsertPartitionFunction>(
            logical_partition_count, hash_method, std::vector<TExpr> {}, std::move(fields));
    RETURN_IF_ERROR(function->init({}));
    *partition_function = std::move(function);
    return Status::OK();
}

Status create_legacy_external_partition_function(
        const TExternalTableSinkHashPartitionInfo& partition_info,
        PartitionerBase::HashValType logical_partition_count, ShuffleHashMethod hash_method,
        const std::vector<TExpr>& partition_exprs,
        std::unique_ptr<PartitionFunction>* const partition_function) {
    if (!partition_info.__isset.algorithm) {
        return Status::InvalidArgument("External sink partition function is missing");
    }
    switch (partition_info.algorithm) {
    case TExternalTableSinkHashAlgorithm::DIRECT_HASH: {
        if (partition_info.__isset.partition_transforms ||
            partition_info.__isset.paimon_fixed_bucket_info) {
            return Status::InvalidArgument(
                    "Legacy direct external sink hash contains incompatible metadata");
        }
        auto function =
                std::make_unique<HashPartitionFunction>(logical_partition_count, hash_method);
        RETURN_IF_ERROR(function->init(partition_exprs));
        *partition_function = std::move(function);
        return Status::OK();
    }
    case TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM: {
        if (partition_info.__isset.paimon_fixed_bucket_info) {
            return Status::InvalidArgument(
                    "Legacy Iceberg sink routing contains incompatible Paimon metadata");
        }
        if (!partition_info.__isset.partition_transforms ||
            partition_info.partition_transforms.size() != partition_exprs.size()) {
            return Status::InvalidArgument(
                    "Legacy Iceberg sink partition transforms must match expression count");
        }
        std::vector<TIcebergPartitionField> fields;
        fields.reserve(partition_exprs.size());
        for (size_t index = 0; index < partition_exprs.size(); ++index) {
            TIcebergPartitionField field;
            field.__set_transform(partition_info.partition_transforms[index]);
            field.__set_source_expr(partition_exprs[index]);
            fields.emplace_back(std::move(field));
        }
        auto function = std::make_unique<IcebergInsertPartitionFunction>(
                logical_partition_count, hash_method, std::vector<TExpr> {}, std::move(fields));
        RETURN_IF_ERROR(function->init({}));
        *partition_function = std::move(function);
        return Status::OK();
    }
    case TExternalTableSinkHashAlgorithm::PAIMON_FIXED_BUCKET: {
        if (partition_info.__isset.partition_transforms) {
            return Status::InvalidArgument(
                    "Legacy Paimon fixed-bucket routing contains incompatible Iceberg metadata");
        }
        if (!partition_info.__isset.paimon_fixed_bucket_info) {
            return Status::InvalidArgument(
                    "Legacy Paimon fixed-bucket routing metadata is missing");
        }
        const auto& info = partition_info.paimon_fixed_bucket_info;
        return create_paimon_fixed_bucket_function(
                logical_partition_count, info.num_buckets, info.partition_field_indexes,
                info.bucket_field_indexes, partition_exprs, partition_function);
    }
    default:
        return Status::InvalidArgument("Unsupported legacy external sink hash algorithm {}",
                                       static_cast<int>(partition_info.algorithm));
    }
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
    if (!partition_info.__isset.partition_function) {
        return create_legacy_external_partition_function(partition_info, logical_partition_count,
                                                         hash_method, partition_exprs,
                                                         partition_function);
    }
    if (partition_info.partition_function == "paimon_fixed_bucket") {
        return create_paimon_fixed_bucket_function(partition_info, logical_partition_count,
                                                   partition_exprs, partition_function);
    }
    if (partition_info.partition_function == "iceberg_transform") {
        return create_iceberg_transform_function(partition_info, logical_partition_count,
                                                 hash_method, partition_exprs, partition_function);
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
