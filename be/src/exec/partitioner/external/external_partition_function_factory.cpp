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

namespace doris {

Status create_external_partition_function(const TExternalTableSinkHashPartitionInfo& partition_info,
                                          PartitionerBase::HashValType logical_partition_count,
                                          ShuffleHashMethod hash_method,
                                          const std::vector<TExpr>& partition_exprs,
                                          std::unique_ptr<PartitionFunction>* partition_function) {
    if (partition_function == nullptr) {
        return Status::InvalidArgument("External partition function output is null");
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
