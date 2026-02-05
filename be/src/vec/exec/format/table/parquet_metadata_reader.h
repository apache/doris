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

#include <gen_cpp/PlanNodes_types.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
namespace io {
class FileReader;
} // namespace io
} // namespace doris

namespace doris::vectorized {
class Block;

// Lightweight reader that surfaces Parquet footer metadata as a table-valued scan.
// It reads only file footers (no data pages) and emits either schema rows or
// row-group/column statistics based on `mode`.
class ParquetMetadataReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(ParquetMetadataReader);

public:
    class ModeHandler;

    ParquetMetadataReader(std::vector<SlotDescriptor*> slots, RuntimeState* state,
                          RuntimeProfile* profile, TMetaScanRange scan_range);
    ~ParquetMetadataReader() override;

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status close() override;

private:
    Status _init_from_scan_range(const TMetaScanRange& scan_range);
    Status _build_rows(std::vector<MutableColumnPtr>& columns);
    Status _append_file_rows(const std::string& path, std::vector<MutableColumnPtr>& columns);

    enum class Mode { SCHEMA, METADATA, FILE_METADATA, KEY_VALUE_METADATA, BLOOM_PROBE };

    RuntimeState* _state = nullptr;
    std::vector<SlotDescriptor*> _slots;
    TMetaScanRange _scan_range;
    std::vector<std::string> _paths;
    // File system type and properties for remote Parquet access.
    TFileType::type _file_type = TFileType::FILE_LOCAL;
    std::map<std::string, std::string> _properties;
    std::string _mode;
    Mode _mode_type = Mode::METADATA;
    std::string _bloom_column;
    std::string _bloom_literal;
    bool _eof = false;
    std::unique_ptr<ModeHandler> _mode_handler;
};

} // namespace doris::vectorized
