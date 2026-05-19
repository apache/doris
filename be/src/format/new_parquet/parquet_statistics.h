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

#include <vector>

#include "common/status.h"
#include "format/reader/file_reader.h"

namespace parquet {
class FileMetaData;
} // namespace parquet

namespace doris::parquet {

// Parquet file-local statistics/page index/bloom filter 裁剪入口。
// 当前阶段保守返回全部 row group；后续所有基于 Parquet metadata 的 pruning 都放在这里，
// 避免污染 ParquetReader 的 scan 调度代码。
Status select_row_groups_by_statistics(const ::parquet::FileMetaData& metadata,
                                       const reader::FileScanRequest& request,
                                       std::vector<int>* selected_row_groups);

} // namespace doris::parquet
