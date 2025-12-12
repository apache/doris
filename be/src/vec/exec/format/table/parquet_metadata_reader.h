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

#include <array>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
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
class FieldDescriptor;
struct FieldSchema;
class FileMetaData;

// Lightweight reader that surfaces Parquet footer metadata as a table-valued scan.
// It reads only file footers (no data pages) and emits either schema rows or
// row-group/column statistics based on `mode`.
class ParquetMetadataReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(ParquetMetadataReader);

public:
    ParquetMetadataReader(std::vector<SlotDescriptor*> slots, RuntimeState* state,
                          RuntimeProfile* profile, TMetaScanRange scan_range);

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status close() override;

private:
    // Initialize projection maps from slot names to output positions.
    void _init_slot_pos_map();
    Status _init_from_scan_range(const TMetaScanRange& scan_range);
    Status _build_rows(std::vector<MutableColumnPtr>& columns);
    Status _append_file_rows(const std::string& path, std::vector<MutableColumnPtr>& columns);
    Status _append_schema_rows(const std::string& path, FileMetaData* metadata,
                               std::vector<MutableColumnPtr>& columns);
    Status _append_schema_field(const std::string& path, const FieldSchema& field,
                                const std::string& current_path,
                                std::vector<MutableColumnPtr>& columns);
    Status _append_metadata_rows(const std::string& path, FileMetaData* metadata,
                                 std::vector<MutableColumnPtr>& columns);

    enum class Mode { SCHEMA, METADATA };

    std::vector<SlotDescriptor*> _slots;
    TMetaScanRange _scan_range;
    std::vector<std::string> _paths;
    // File system type and properties for remote Parquet access.
    TFileType::type _file_type = TFileType::FILE_LOCAL;
    std::map<std::string, std::string> _properties;
    std::string _mode;
    Mode _mode_type = Mode::METADATA;
    bool _eof = false;

    static constexpr size_t kSchemaColumnCount = 11;
    static constexpr size_t kMetadataColumnCount = 21;
    // Maps full schema/metadata column index -> position in `_slots` (or -1 if not requested).
    std::array<int, kSchemaColumnCount> _schema_slot_pos;
    std::array<int, kMetadataColumnCount> _metadata_slot_pos;
};

} // namespace doris::vectorized
