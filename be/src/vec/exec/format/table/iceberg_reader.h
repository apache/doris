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
#include "exec/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "table_format_reader.h"
#include "vec/columns/column_dictionary.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/table/equality_delete.h"
#include "vec/exprs/vslot_ref.h"

namespace tparquet {
class KeyValue;
} // namespace tparquet

namespace doris {
#include "common/compile_check_begin.h"
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
namespace vectorized {
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;
class Block;
class GenericReader;
class ShardedKVCache;
class VExprContext;

class IcebergTableReader : public TableFormatReader, public TableSchemaChangeHelper {
public:
    struct PositionDeleteRange {
        std::vector<std::string> data_file_path;
        std::vector<std::pair<int, int>> range;
    };

    IcebergTableReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                       RuntimeState* state, const TFileScanRangeParams& params,
                       const TFileRangeDesc& range, ShardedKVCache* kv_cache, io::IOContext* io_ctx,
                       FileMetaCache* meta_cache);
    ~IcebergTableReader() override = default;

    Status init_row_filters() final;

    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

    enum { DATA, POSITION_DELETE, EQUALITY_DELETE };
    enum Fileformat { NONE, PARQUET, ORC, AVRO };

    virtual void set_delete_rows() = 0;

protected:
    struct IcebergProfile {
        RuntimeProfile::Counter* num_delete_files;
        RuntimeProfile::Counter* num_delete_rows;
        RuntimeProfile::Counter* delete_files_read_time;
        RuntimeProfile::Counter* delete_rows_sort_time;
    };
    using DeleteRows = std::vector<int64_t>;
    using DeleteFile = phmap::parallel_flat_hash_map<
            std::string, std::unique_ptr<DeleteRows>, std::hash<std::string>, std::equal_to<>,
            std::allocator<std::pair<const std::string, std::unique_ptr<DeleteRows>>>, 8,
            std::mutex>;
    /**
     * https://iceberg.apache.org/spec/#position-delete-files
     * The rows in the delete file must be sorted by file_path then position to optimize filtering rows while scanning.
     * Sorting by file_path allows filter pushdown by file in columnar storage formats.
     * Sorting by position allows filtering rows while scanning, to avoid keeping deletes in memory.
     */
    void _sort_delete_rows(std::vector<std::vector<int64_t>*>& delete_rows_array,
                           int64_t num_delete_rows);

    PositionDeleteRange _get_range(const ColumnDictI32& file_path_column);

    PositionDeleteRange _get_range(const ColumnString& file_path_column);

    static std::string _delet_file_cache_key(const std::string& path) { return "delete_" + path; }

    Status _position_delete_base(const std::string data_file_path,
                                 const std::vector<TIcebergDeleteFileDesc>& delete_files);
    Status _equality_delete_base(const std::vector<TIcebergDeleteFileDesc>& delete_files);
    virtual std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) = 0;
    void _generate_equality_delete_block(Block* block,
                                         const std::vector<std::string>& equality_delete_col_names,
                                         const std::vector<DataTypePtr>& equality_delete_col_types);
    // Equality delete should read the primary columns. Add the missing columns
    Status _expand_block_if_need(Block* block);
    // Remove the added delete columns
    Status _shrink_block_if_need(Block* block);

    // owned by scan node
    ShardedKVCache* _kv_cache;
    IcebergProfile _iceberg_profile;
    std::vector<int64_t> _iceberg_delete_rows;
    std::vector<std::string> _expand_col_names;
    std::vector<ColumnWithTypeAndName> _expand_columns;
    std::vector<std::string> _all_required_col_names;

    Fileformat _file_format = Fileformat::NONE;

    const int64_t MIN_SUPPORT_DELETE_FILES_VERSION = 2;
    const std::string ICEBERG_ROW_POS = "pos";
    const std::string ICEBERG_FILE_PATH = "file_path";
    const std::vector<std::string> delete_file_col_names {ICEBERG_ROW_POS, ICEBERG_FILE_PATH};
    const int ICEBERG_FILE_PATH_INDEX = 0;
    const int ICEBERG_FILE_POS_INDEX = 1;
    const int READ_DELETE_FILE_BATCH_SIZE = 102400;

    //Read position_delete_file  TFileRangeDesc, generate DeleteFile
    virtual Status _read_position_delete_file(const TFileRangeDesc*, DeleteFile*) = 0;

    void _gen_position_delete_file_range(Block& block, DeleteFile* const position_delete,
                                         size_t read_rows, bool file_path_column_dictionary_coded);

    // equality delete
    Block _equality_delete_block;
    std::unique_ptr<EqualityDeleteBase> _equality_delete_impl;
};

class IcebergParquetReader final : public IcebergTableReader {
public:
    ENABLE_FACTORY_CREATOR(IcebergParquetReader);

    IcebergParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                         RuntimeState* state, const TFileScanRangeParams& params,
                         const TFileRangeDesc& range, ShardedKVCache* kv_cache,
                         io::IOContext* io_ctx, FileMetaCache* meta_cache)
            : IcebergTableReader(std::move(file_format_reader), profile, state, params, range,
                                 kv_cache, io_ctx, meta_cache) {}
    Status init_reader(
            const std::vector<std::string>& file_col_names,
            const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);

    Status _read_position_delete_file(const TFileRangeDesc* delete_range,
                                      DeleteFile* position_delete) final;

    void set_delete_rows() final {
        auto* parquet_reader = (ParquetReader*)(_file_format_reader.get());
        parquet_reader->set_delete_rows(&_iceberg_delete_rows);
    }

protected:
    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) final {
        return ParquetReader::create_unique(_profile, _params, delete_desc,
                                            READ_DELETE_FILE_BATCH_SIZE,
                                           &_state->timezone_obj(),
                                            _io_ctx, _state, _meta_cache);
    }
};
class IcebergOrcReader final : public IcebergTableReader {
public:
    ENABLE_FACTORY_CREATOR(IcebergOrcReader);

    Status _read_position_delete_file(const TFileRangeDesc* delete_range,
                                      DeleteFile* position_delete) final;

    IcebergOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                     RuntimeState* state, const TFileScanRangeParams& params,
                     const TFileRangeDesc& range, ShardedKVCache* kv_cache, io::IOContext* io_ctx,
                     FileMetaCache* meta_cache)
            : IcebergTableReader(std::move(file_format_reader), profile, state, params, range,
                                 kv_cache, io_ctx, meta_cache) {}

    void set_delete_rows() final {
        auto* orc_reader = (OrcReader*)_file_format_reader.get();
        orc_reader->set_position_delete_rowids(&_iceberg_delete_rows);
    }

    Status init_reader(
            const std::vector<std::string>& file_col_names,
            const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);

protected:
    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) override {
        return OrcReader::create_unique(_profile, _state, _params, delete_desc,
                                        READ_DELETE_FILE_BATCH_SIZE, _state->timezone(), _io_ctx,
                                        _meta_cache);
    }

private:
    static const std::string ICEBERG_ORC_ATTRIBUTE;
};

} // namespace vectorized
#include "common/compile_check_end.h"
} // namespace doris
