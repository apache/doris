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

#include <memory>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/compaction.h"

namespace doris {

class CloudBaseCompaction : public CloudCompactionMixin {
public:
    CloudBaseCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);
    ~CloudBaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    void do_lease();

private:
    Status pick_rowsets_to_compact();

    std::string_view compaction_name() const override { return "CloudBaseCompaction"; }

    Status modify_rowsets() override;

    void garbage_collection() override;

    void _filter_input_rowset();

    void build_basic_info();

    ReaderType compaction_type() const override { return ReaderType::READER_BASE_COMPACTION; }

    std::string _uuid;
    int64_t _input_segments = 0;
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
};

} // namespace doris
