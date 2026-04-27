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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column_dictionary.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/iceberg_reader_mixin.h"
#include "storage/olap_common.h"

namespace tparquet {
class KeyValue;
class ColumnMetaData;
} // namespace tparquet

namespace doris {
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;
class TIcebergDeleteFileDesc;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;
class Block;
class GenericReader;
class ShardedKVCache;
class VExprContext;

struct IcebergTableReader {
    static bool _is_fully_dictionary_encoded(const tparquet::ColumnMetaData& column_metadata);
};

// IcebergParquetReader: inherits ParquetReader via IcebergReaderMixin CRTP
class IcebergParquetReader final : public IcebergReaderMixin<ParquetReader> {
public:
    ENABLE_FACTORY_CREATOR(IcebergParquetReader);

    IcebergParquetReader(ShardedKVCache* kv_cache, RuntimeProfile* profile,
                         const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         size_t batch_size, const cctz::time_zone* ctz, io::IOContext* io_ctx,
                         RuntimeState* state, FileMetaCache* meta_cache)
            : IcebergReaderMixin<ParquetReader>(kv_cache, profile, params, range, batch_size, ctz,
                                                io_ctx, state, meta_cache) {}

    void set_delete_rows() final {
        // Call ParquetReader's set_delete_rows(const vector<int64_t>*)
        ParquetReader::set_delete_rows(_iceberg_delete_rows);
    }

protected:
    // Parquet-specific schema matching via on_before_init_reader hook
    Status on_before_init_reader(ReaderInitContext* ctx) override;

    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) final {
        return ParquetReader::create_unique(this->get_profile(), this->get_scan_params(),
                                            delete_desc, READ_DELETE_FILE_BATCH_SIZE,
                                            &this->get_state()->timezone_obj(), this->get_io_ctx(),
                                            this->get_state(), this->_meta_cache);
    }

    static ColumnIdResult _create_column_ids(const FieldDescriptor* field_desc,
                                             const TupleDescriptor* tuple_descriptor);

private:
    Status _read_position_delete_file(const TFileRangeDesc* delete_range,
                                      DeleteFile* position_delete) final;
};

// IcebergOrcReader: inherits OrcReader via IcebergReaderMixin CRTP
class IcebergOrcReader final : public IcebergReaderMixin<OrcReader> {
public:
    ENABLE_FACTORY_CREATOR(IcebergOrcReader);

    IcebergOrcReader(ShardedKVCache* kv_cache, RuntimeProfile* profile, RuntimeState* state,
                     const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     size_t batch_size, const std::string& ctz, io::IOContext* io_ctx,
                     FileMetaCache* meta_cache)
            : IcebergReaderMixin<OrcReader>(kv_cache, profile, state, params, range, batch_size,
                                            ctz, io_ctx, meta_cache) {}

    void set_delete_rows() final {
        // Call OrcReader's set_position_delete_rowids
        this->set_position_delete_rowids(_iceberg_delete_rows);
    }

protected:
    // ORC-specific schema matching via on_before_init_reader hook
    Status on_before_init_reader(ReaderInitContext* ctx) override;

    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) override {
        return OrcReader::create_unique(this->get_profile(), this->get_state(),
                                        this->get_scan_params(), delete_desc,
                                        READ_DELETE_FILE_BATCH_SIZE, this->get_state()->timezone(),
                                        this->get_io_ctx(), this->_meta_cache);
    }

    static ColumnIdResult _create_column_ids(const orc::Type* orc_type,
                                             const TupleDescriptor* tuple_descriptor);

    static const std::string ICEBERG_ORC_ATTRIBUTE;

private:
    Status _read_position_delete_file(const TFileRangeDesc* delete_range,
                                      DeleteFile* position_delete) final;
};

} // namespace doris
