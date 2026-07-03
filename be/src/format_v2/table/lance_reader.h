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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "format_v2/table_reader.h"

using LanceDataset = struct LanceDataset;
using LanceScanner = struct LanceScanner;

namespace arrow {
class RecordBatch;
} // namespace arrow

namespace doris::format::lance {

class LanceTableReader final : public format::TableReader {
public:
    ~LanceTableReader() override;

    Status init(format::TableReadOptions&& options) override;
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status close() override;
    std::string debug_string() const override;

private:
    struct DatasetKey {
        std::string uri;
        uint64_t version = 0;
        std::vector<std::string> storage_options;

        std::string debug_string() const;
    };

    Status _ensure_dataset_open();
    Status _open_dataset(const DatasetKey& key);
    Status _open_scanner();
    Status _validate_current_split() const;
    Status _build_column_name_to_output_index();
    Status _read_next_lance_block(Block* block, bool* eos);
    Status _fill_output_block(const arrow::RecordBatch& batch, Block* output_block,
                              size_t* rows) const;
    void _close_scanner();
    void _close_dataset();

    DatasetKey _current_dataset_key() const;
    std::string _dataset_uri() const;
    uint64_t _dataset_version() const;
    Status _fragment_ids(std::vector<uint64_t>* ids) const;
    std::vector<std::string> _storage_option_values() const;

    static bool _dataset_key_equal(const DatasetKey& lhs, const DatasetKey& rhs);
    static Status _lance_error(std::string_view operation);

    cctz::time_zone _ctz;
    LanceDataset* _dataset = nullptr;
    LanceScanner* _scanner = nullptr;
    std::optional<DatasetKey> _dataset_key;
    bool _scanner_opened = false;
    bool _eof = false;
    bool _closed = false;
    std::unordered_map<std::string, size_t> _column_name_to_output_index;
};

} // namespace doris::format::lance
