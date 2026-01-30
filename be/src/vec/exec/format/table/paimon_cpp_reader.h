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

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cctz/time_zone.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/table/source/split.h"
#include "vec/exec/format/generic_reader.h"

namespace paimon {
class TableRead;
class Predicate;
} // namespace paimon

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Block;

class PaimonCppReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(PaimonCppReader);

public:
    PaimonCppReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                    RuntimeProfile* profile, const TFileRangeDesc& range,
                    const TFileScanRangeParams* range_params);
    ~PaimonCppReader() override;

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;
    Status close() override;
    void set_predicate(std::shared_ptr<paimon::Predicate> predicate) {
        _predicate = std::move(predicate);
    }

private:
    Status _init_paimon_reader();
    Status _decode_split(std::shared_ptr<paimon::Split>* split);
    // Resolve paimon table root path for schema/manifest lookup.
    std::optional<std::string> _resolve_table_path() const;
    std::vector<std::string> _build_read_columns() const;
    std::map<std::string, std::string> _build_options() const;

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;
    [[maybe_unused]] RuntimeProfile* _profile = nullptr;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams* _range_params = nullptr;

    std::shared_ptr<paimon::Split> _split;
    std::unique_ptr<paimon::TableRead> _table_read;
    std::unique_ptr<paimon::BatchReader> _batch_reader;
    std::shared_ptr<paimon::Predicate> _predicate;

    std::unordered_map<std::string, uint32_t> _col_name_to_block_idx;
    int64_t _remaining_table_level_row_count = -1;
    cctz::time_zone _ctzz;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
