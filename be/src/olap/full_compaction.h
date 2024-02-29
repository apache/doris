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

#include <butil/macros.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/compaction.h"

namespace doris {

class FullCompaction final : public CompactionMixin {
public:
    FullCompaction(StorageEngine& engine, const TabletSharedPtr& tablet);

    ~FullCompaction() override;

    Status prepare_compact() override;

    Status execute_compact() override;

private:
    Status pick_rowsets_to_compact();

    Status modify_rowsets() override;

    std::string_view compaction_name() const override { return "full compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_FULL_COMPACTION; }

    Status _check_all_version(const std::vector<RowsetSharedPtr>& rowsets);
    Status _full_compaction_update_delete_bitmap(const RowsetSharedPtr& rowset,
                                                 RowsetWriter* rowset_writer);
    Status _full_compaction_calc_delete_bitmap(const RowsetSharedPtr& published_rowset,
                                               const RowsetSharedPtr& rowset,
                                               const int64_t& cur_version,
                                               RowsetWriter* rowset_writer);
};

} // namespace doris
