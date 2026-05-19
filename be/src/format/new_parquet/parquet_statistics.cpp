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

#include "format/new_parquet/parquet_statistics.h"

#include <parquet/api/reader.h>

namespace doris::parquet {

Status select_row_groups_by_statistics(const ::parquet::FileMetaData& metadata,
                                       const reader::FileScanRequest& request,
                                       std::vector<int>* selected_row_groups) {
    (void)request;
    if (selected_row_groups == nullptr) {
        return Status::InvalidArgument("selected_row_groups is null");
    }
    selected_row_groups->clear();
    const int num_row_groups = metadata.num_row_groups();
    selected_row_groups->reserve(num_row_groups);
    for (int row_group_idx = 0; row_group_idx < num_row_groups; ++row_group_idx) {
        selected_row_groups->push_back(row_group_idx);
    }
    return Status::OK();
}

} // namespace doris::parquet
