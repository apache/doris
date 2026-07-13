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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "roaring/roaring64map.hh"

namespace doris {
class SlotDescriptor;
} // namespace doris

namespace doris::format::iceberg {

class IcebergPositionDeleteSysTableV2Reader final : public format::TableReader {
public:
    ~IcebergPositionDeleteSysTableV2Reader() override;

    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status close() override;
    std::string debug_string() const override;

private:
    enum class DeleteFileKind {
        POSITION_DELETE,
        DELETION_VECTOR,
    };

    struct ReadColumn {
        std::string name;
        DataTypePtr type;
    };

    Status _init_split();
    Status _init_position_delete_reader();
    Status _init_deletion_vector_reader();
    Status _append_position_delete_block(Block* output_block, const Block& delete_block,
                                         size_t delete_rows, size_t* appended_rows);
    Status _append_deletion_vector_block(Block* block, size_t* read_rows, bool* eof);
    Status _append_sys_column(MutableColumnPtr& column, const SlotDescriptor& slot,
                              const Block* delete_block, size_t source_row, uint64_t dv_pos);
    Status _append_partition_column(MutableColumnPtr& column, const SlotDescriptor& slot);
    Block _create_delete_block() const;
    bool _output_column_requested(const std::string& name) const;
    void _init_read_columns(bool read_row);
    Status _build_delete_file_projected_columns(std::vector<ColumnDefinition>* columns) const;
    const std::string& _delete_file_output_path() const;

    TFileRangeDesc _current_range;
    const TIcebergFileDesc* _iceberg_file_desc = nullptr;
    const TIcebergDeleteFileDesc* _delete_file_desc = nullptr;
    DeleteFileKind _delete_file_kind = DeleteFileKind::POSITION_DELETE;
    std::unique_ptr<format::TableReader> _position_reader;
    std::vector<ReadColumn> _read_columns;
    ColumnPtr _partition_value;
    roaring::Roaring64Map _dv_positions;
    std::optional<roaring::Roaring64Map::const_iterator> _next_dv_position;
    bool _has_split = false;
};

} // namespace doris::format::iceberg
