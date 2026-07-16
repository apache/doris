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

#include "common/status.h"
#include "format_v2/table_reader.h"

struct LanceBatch;
struct LanceDataset;
struct LanceScanner;

namespace doris::format::lance {

// A FORMAT_LANCE table reader. Unlike file formats such as Parquet, a Lance split is not a
// physical-file range. It is a set of fragments in one fixed dataset snapshot, so the dataset is
// owned by this table reader and each split owns only its scanner.
class LanceTableReader final : public TableReader {
public:
    ~LanceTableReader() override;

    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status abort_split() override;
    Status close() override;

private:
    struct DatasetKey {
        std::string uri;
        int64_t version = 0;
        std::vector<std::string> storage_options;
        bool operator==(const DatasetKey&) const = default;
    };

    Status _validate_split(const TFileRangeDesc& range) const;
    Status _open_dataset(const DatasetKey& key);
    Status _open_scanner(const TFileRangeDesc& range);
    void _close_scanner();
    void _close_dataset();
    Status _fill_block_from_arrow(LanceBatch* batch, Block* block, size_t* rows);
    Status _prepare_conjuncts();
    std::vector<std::string> _storage_options() const;
    DatasetKey _dataset_key(const TFileRangeDesc& range) const;
    static Status _lance_error(std::string_view operation);

    LanceDataset* _dataset = nullptr;
    LanceScanner* _scanner = nullptr;
    std::optional<DatasetKey> _opened_dataset_key;
    std::unordered_map<std::string, size_t> _output_name_to_idx;
    cctz::time_zone _ctz;
    size_t _scanner_batch_size = 0;
    bool _eof = false;
};

} // namespace doris::format::lance
