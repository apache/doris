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

#include <string>
#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/compaction.h"

namespace doris {

// BaseCompaction is derived from Compaction.
// BaseCompaction will implements
//   1. its policy to pick rowsets
//   2. do compaction to produce new rowset.

class BaseCompaction final : public CompactionMixin {
public:
    BaseCompaction(StorageEngine& engine, const TabletSharedPtr& tablet);
    ~BaseCompaction() override;

    Status prepare_compact() override;

    Status execute_compact() override;

private:
    Status pick_rowsets_to_compact();
    std::string_view compaction_name() const override { return "base compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_BASE_COMPACTION; }

    // filter input rowset in some case:
    // 1. dup key without delete predicate
    void _filter_input_rowset();
};

} // namespace doris
