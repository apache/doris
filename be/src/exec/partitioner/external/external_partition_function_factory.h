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

#pragma once

#include <gen_cpp/Partitions_types.h>

#include <memory>
#include <vector>

#include "exec/partitioner/partitioner.h"

namespace doris {
#include "common/compile_check_begin.h"

// Validates connector-specific routing metadata and creates the matching logical partition
// function. Writer assignment remains the responsibility of ExternalTableSinkHashPartitioner.
Status create_external_partition_function(const TExternalTableSinkHashPartitionInfo& partition_info,
                                          PartitionerBase::HashValType logical_partition_count,
                                          ShuffleHashMethod hash_method,
                                          const std::vector<TExpr>& partition_exprs,
                                          std::unique_ptr<PartitionFunction>* partition_function);

#include "common/compile_check_end.h"
} // namespace doris
