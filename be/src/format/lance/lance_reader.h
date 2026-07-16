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

#include <cctz/time_zone.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "format/generic_reader.h"

struct LanceDataset;
struct LanceScanner;
struct LanceBatch;

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
} // namespace doris

namespace arrow {
class Schema;
} // namespace arrow

namespace doris {
#include "common/compile_check_begin.h"

class Block;

// Native Lance-C reader for a fixed Lance snapshot and a disjoint set of fragments.
class LanceReader final : public GenericReader {
    ENABLE_FACTORY_CREATOR(LanceReader);

public:
    LanceReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                RuntimeProfile* profile, const TFileRangeDesc& range,
                const TFileScanRangeParams* scan_params);
    ~LanceReader() override;

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;
    Status init_schema_reader() override;
    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override;
    Status close() override;

private:
    Status _validate_range(bool require_fragment_ids = true) const;
    Status _open_dataset();
    Status _open_scanner();
    Status _fill_block_from_arrow(LanceBatch* batch, Block* block, size_t* read_rows);
    std::vector<std::string> _storage_options() const;
    static Status _lance_error(std::string_view operation);

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;
    [[maybe_unused]] RuntimeProfile* _profile = nullptr;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams* _scan_params = nullptr;

    LanceDataset* _dataset = nullptr;
    LanceScanner* _scanner = nullptr;
    std::shared_ptr<arrow::Schema> _schema;
    std::vector<std::string> _projected_columns;
    std::unordered_map<std::string, uint32_t> _block_name_to_idx;
    cctz::time_zone _ctzz;
    bool _eof = false;
};

#include "common/compile_check_end.h"
} // namespace doris
