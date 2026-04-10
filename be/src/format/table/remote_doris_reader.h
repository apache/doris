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

#include <arrow/flight/client.h>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "format/jni/jni_reader.h"
#include "storage/olap_scan_common.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class Block;
} // namespace doris

namespace doris {
class RemoteDorisReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(RemoteDorisReader);

public:
    RemoteDorisReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                      RuntimeProfile* profile, const TFileRangeDesc& range);

    ~RemoteDorisReader() override = default;

    Status init_reader();

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status close() override;

    /**
     * Set column name to block index map from FileScanner to avoid repeated map creation.
     */
    void set_col_name_to_block_idx(
            std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
        _col_name_to_block_idx = col_name_to_block_idx;
    }

private:
    arrow::Status init_stream();
    const TFileRangeDesc& _range;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    cctz::time_zone _ctzz;
    std::unique_ptr<arrow::flight::FlightClient> _flight_client;
    std::unique_ptr<arrow::flight::FlightStreamReader> _stream;
    // Column name to block index map, passed from FileScanner to avoid repeated map creation
    std::unordered_map<std::string, uint32_t>* _col_name_to_block_idx = nullptr;
};
} // namespace doris
