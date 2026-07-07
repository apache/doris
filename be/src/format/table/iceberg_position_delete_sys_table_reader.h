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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "format/generic_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "runtime/descriptors.h"

namespace doris {

class FileMetaCache;
class RuntimeProfile;
class RuntimeState;

class IcebergPositionDeleteSysTableReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(IcebergPositionDeleteSysTableReader);

public:
    IcebergPositionDeleteSysTableReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                        RuntimeState* state, RuntimeProfile* profile,
                                        const TFileRangeDesc& range,
                                        const TFileScanRangeParams* range_params,
                                        FileMetaCache* meta_cache);
    ~IcebergPositionDeleteSysTableReader() override;

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;
    void set_batch_size(size_t batch_size) override { _batch_size = batch_size; }
    size_t get_batch_size() const override { return _batch_size; }
    Status close() override;

protected:
    Status _do_init_reader(ReaderInitContext* ctx) override;
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

private:
    enum class DeleteFileKind {
        POSITION_DELETE,
        DELETION_VECTOR,
    };

    struct ReadColumn {
        std::string name;
        DataTypePtr type;
    };

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
    const std::string& _delete_file_output_path() const;

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams* _range_params = nullptr;

    IcebergDeleteFileIOContext _io_context;
    const TIcebergFileDesc* _iceberg_file_desc = nullptr;
    const TIcebergDeleteFileDesc* _delete_file_desc = nullptr;
    DeleteFileKind _delete_file_kind = DeleteFileKind::POSITION_DELETE;

    size_t _batch_size = 102400;
    std::unique_ptr<GenericReader> _position_reader;
    std::vector<ReadColumn> _read_columns;
    std::unordered_map<std::string, uint32_t> _read_col_name_to_block_idx;
    roaring::Roaring64Map _dv_positions;
    std::optional<roaring::Roaring64Map::const_iterator> _next_dv_position;
};

} // namespace doris
