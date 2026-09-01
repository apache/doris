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

#include "common/status.h"
#include "exec/partitioner/external/paimon_fixed_bucket_partition_function.h"
#include "format/transformer/iceberg_partition_function.h"

namespace doris {

namespace {
bool has_partition_transform_metadata(const TExternalTableSinkHashPartitionInfo& info) {
    return info.__isset.partition_transforms;
}

bool has_paimon_metadata(const TExternalTableSinkHashPartitionInfo& info) {
    return info.__isset.paimon_fixed_bucket_info;
}

Status create_direct_hash_function(const TExternalTableSinkHashPartitionInfo& info,
                                   PartitionerBase::HashValType logical_partition_count,
                                   ShuffleHashMethod hash_method,
                                   const std::vector<TExpr>& partition_exprs,
                                   std::unique_ptr<PartitionFunction>* partition_function) {
    if (has_partition_transform_metadata(info) || has_paimon_metadata(info)) {
        return Status::InvalidArgument("Direct external sink hash contains incompatible metadata");
    }
    auto function = std::make_unique<HashPartitionFunction>(logical_partition_count, hash_method);
    RETURN_IF_ERROR(function->init(partition_exprs));
    *partition_function = std::move(function);
    return Status::OK();
}

Status create_iceberg_function(const TExternalTableSinkHashPartitionInfo& info,
                               PartitionerBase::HashValType logical_partition_count,
                               ShuffleHashMethod hash_method,
                               const std::vector<TExpr>& partition_exprs,
                               std::unique_ptr<PartitionFunction>* partition_function) {
    if (has_paimon_metadata(info)) {
        return Status::InvalidArgument(
                "Iceberg external sink routing contains incompatible Paimon metadata");
    }
    if (!info.__isset.partition_transforms) {
        return Status::InvalidArgument("Iceberg external sink partition transforms are missing");
    }
    if (info.partition_transforms.size() != partition_exprs.size()) {
        return Status::InvalidArgument(
                "External sink partition transform count {} does not match expression count {}",
                info.partition_transforms.size(), partition_exprs.size());
    }
    std::vector<TIcebergPartitionField> fields;
    fields.reserve(partition_exprs.size());
    for (size_t index = 0; index < partition_exprs.size(); ++index) {
        TIcebergPartitionField field;
        field.__set_transform(info.partition_transforms[index]);
        field.__set_source_expr(partition_exprs[index]);
        fields.emplace_back(std::move(field));
    }
    auto function = std::make_unique<IcebergInsertPartitionFunction>(
            logical_partition_count, hash_method, std::vector<TExpr> {}, std::move(fields));
    RETURN_IF_ERROR(function->init({}));
    *partition_function = std::move(function);
    return Status::OK();
}

Status create_paimon_fixed_bucket_function(const TExternalTableSinkHashPartitionInfo& info,
                                           PartitionerBase::HashValType logical_partition_count,
                                           const std::vector<TExpr>& partition_exprs,
                                           std::unique_ptr<PartitionFunction>* partition_function) {
    if (!info.__isset.paimon_fixed_bucket_info) {
        return Status::InvalidArgument("Paimon fixed-bucket routing metadata is missing");
    }
    if (has_partition_transform_metadata(info)) {
        return Status::InvalidArgument(
                "Paimon fixed-bucket routing contains incompatible metadata");
    }
    auto function = std::make_unique<PaimonFixedBucketPartitionFunction>(
            logical_partition_count, info.paimon_fixed_bucket_info);
    RETURN_IF_ERROR(function->init(partition_exprs));
    *partition_function = std::move(function);
    return Status::OK();
}

} // namespace

Status create_external_partition_function(const TExternalTableSinkHashPartitionInfo& partition_info,
                                          PartitionerBase::HashValType logical_partition_count,
                                          ShuffleHashMethod hash_method,
                                          const std::vector<TExpr>& partition_exprs,
                                          std::unique_ptr<PartitionFunction>* partition_function) {
    if (partition_function == nullptr) {
        return Status::InvalidArgument("External partition function output is null");
    }
    switch (partition_info.algorithm) {
    case TExternalTableSinkHashAlgorithm::DIRECT_HASH:
        return create_direct_hash_function(partition_info, logical_partition_count, hash_method,
                                           partition_exprs, partition_function);
    case TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM:
        return create_iceberg_function(partition_info, logical_partition_count, hash_method,
                                       partition_exprs, partition_function);
    case TExternalTableSinkHashAlgorithm::PAIMON_FIXED_BUCKET:
        return create_paimon_fixed_bucket_function(partition_info, logical_partition_count,
                                                   partition_exprs, partition_function);
    default:
        return Status::InvalidArgument("Unsupported external sink hash algorithm {}",
                                       static_cast<int>(partition_info.algorithm));
    }
}

} // namespace doris
