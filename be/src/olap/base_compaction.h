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

#include "olap/compaction.h"

namespace doris {

// BaseCompaction is derived from Compaction.
// BaseCompaction will implements
//   1. its policy to pick rowsets
//   2. do compaction to produce new rowset.

class BaseCompaction : public Compaction {
public:
    BaseCompaction(TabletSharedPtr tablet);
    ~BaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact_impl() override;

    std::vector<RowsetSharedPtr> get_input_rowsets() { return _input_rowsets; }

protected:
    Status pick_rowsets_to_compact() override;
    std::string compaction_name() const override { return "base compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_BASE_COMPACTION; }

private:
    // check if all input rowsets are non overlapping among segments.
    // a rowset with overlapping segments should be compacted by cumulative compaction first.
    Status _check_rowset_overlapping(const vector<RowsetSharedPtr>& rowsets);

    DISALLOW_COPY_AND_ASSIGN(BaseCompaction);
};

} // namespace doris
