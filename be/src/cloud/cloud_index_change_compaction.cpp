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

#include "cloud/cloud_index_change_compaction.h"

namespace doris {

CloudIndexChangeCompaction::~CloudIndexChangeCompaction() = default;

CloudIndexChangeCompaction::CloudIndexChangeCompaction(CloudStorageEngine& engine,
                                                       CloudTabletSPtr tablet,
                                                       RowsetSharedPtr input_rowset,
                                                       TabletSchemaSPtr output_schema)
        : CloudFullCompaction(engine, tablet) {
    _input_rowsets.push_back(input_rowset);
    _output_schema = output_schema;
}

Status CloudIndexChangeCompaction::pick_rowsets_to_compact() {
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _base_compaction_cnt = cloud_tablet()->base_compaction_cnt();
        _cumulative_compaction_cnt = cloud_tablet()->cumulative_compaction_cnt();
    }
    return Status::OK();
}

} // namespace doris