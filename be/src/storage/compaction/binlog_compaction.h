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

#include <cstdint>
#include <string_view>

#include "common/status.h"
#include "io/io_common.h"
#include "storage/compaction/compaction.h"

namespace doris {

class BinlogCompaction final : public CompactionMixin {
public:
    BinlogCompaction(StorageEngine& engine, const TabletSharedPtr& tablet, int8_t compaction_level);
    ~BinlogCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    std::optional<CompactionProfileType> profile_type() const override {
        return CompactionProfileType::BINLOG;
    }

    int8_t compaction_level() const override { return _compaction_level; }

protected:
    Status modify_rowsets() override;

    std::string_view compaction_name() const override { return "binlog compaction"; }

    ReaderType compaction_type() const override { return ReaderType::READER_BINLOG_COMPACTION; }

private:
    Status pick_rowsets_to_compact();

    int8_t _compaction_level = -1;
};

} // namespace doris
