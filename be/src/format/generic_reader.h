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

#include "common/status.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/descriptors.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/segment/common.h"
#include "storage/segment/condition_cache.h"
#include "util/profile_collector.h"

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris {
#include "common/compile_check_begin.h"

class Block;
class VSlotRef;

/// Base context for the unified init_reader(ReaderInitContext*) template method.
/// Contains fields shared by ALL reader types. Format-specific readers define
/// subclasses (ParquetInitContext, OrcInitContext, etc.) with extra fields.
/// FileScanner allocates the appropriate subclass and populates the shared fields
/// before calling init_reader().
struct ReaderInitContext {
    virtual ~ReaderInitContext() = default;

    // ---- Owned by FileScanner, shared by all readers ----
    const std::vector<ColumnDescriptor>* column_descs = nullptr;
    std::unordered_map<std::string, uint32_t>* col_name_to_block_idx = nullptr;
    RuntimeState* state = nullptr;
    const TupleDescriptor* tuple_descriptor = nullptr;
    const RowDescriptor* row_descriptor = nullptr;
    const TFileScanRangeParams* params = nullptr;
    const TFileRangeDesc* range = nullptr;
    TPushAggOp::type push_down_agg_type = TPushAggOp::type::NONE;

    // ---- Output slots (populated by on_before_init_reader, consumed by _do_init_reader) ----
    // column_names: the list of file columns to read. Populated by on_before_init_reader
    // from column_descs (slot→name mapping). _do_init_reader uses this to configure the
    // format-specific parsing engine. For standalone readers (column_descs==nullptr),
    // callers populate column_names directly before calling init_reader.
    std::vector<std::string> column_names;
    std::shared_ptr<TableSchemaChangeHelper::Node> table_info_node =
            TableSchemaChangeHelper::ConstNode::get_instance();
    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;
};

/// Safe downcast for ReaderInitContext subclasses.
/// Uses dynamic_cast + DORIS_CHECK: crashes on type mismatch (per Doris coding standards).
template <typename To, typename From>
To* checked_context_cast(From* ptr) {
    auto* result = dynamic_cast<To*>(ptr);
    DORIS_CHECK(result != nullptr);
    return result;
}

/// Base reader interface for all file readers.
/// A GenericReader is responsible for reading a file and returning
/// a set of blocks with specified schema.
///
/// Provides hook virtual methods that implement the Template Method pattern:
///   init_reader:      _open_file_reader → on_before_init_reader → _do_init_reader → on_after_init_reader
///   get_next_block:   on_before_read_block → _do_get_next_block → on_after_read_block
///
/// Column-filling logic (partition/missing/synthesized) lives in TableFormatReader.
class GenericReader : public ProfileCollector {
public:
    GenericReader() : _push_down_agg_type(TPushAggOp::type::NONE) {}
    void set_push_down_agg_type(TPushAggOp::type push_down_agg_type) {
        _push_down_agg_type = push_down_agg_type;
    }

    virtual Status get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;

    // Override this in readers that can adjust batch size between consecutive reads.
    virtual void set_batch_size(size_t batch_size) {}
    virtual size_t get_batch_size() const { return 0; }

    // Type is always nullable to process illegal values.
    virtual Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                               std::unordered_set<std::string>* missing_cols) {
        return Status::NotSupported("get_columns is not implemented");
    }

    // This method is responsible for initializing the resource for parsing schema.
    // It will be called before `get_parsed_schema`.
    virtual Status init_schema_reader() {
        return Status::NotSupported("init_schema_reader is not implemented for this reader.");
    }
    // `col_types` is always nullable to process illegal values.
    virtual Status get_parsed_schema(std::vector<std::string>* col_names,
                                     std::vector<DataTypePtr>* col_types) {
        return Status::NotSupported("get_parsed_schema is not implemented for this reader.");
    }
    ~GenericReader() override = default;

    /// If the underlying FileReader has filled the partition&missing columns,
    /// The FileScanner does not need to fill
    virtual bool fill_all_columns() const { return _fill_all_columns; }

    /// Tell the underlying FileReader the partition&missing columns,
    /// and the FileReader determine to fill columns or not.
    /// Should set _fill_all_columns = true, if fill the columns.
    virtual Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns,
            const std::unordered_map<std::string, bool>& partition_value_is_null = {}) {
        return Status::OK();
    }

    virtual Status close() { return Status::OK(); }

    Status read_by_rows(const std::list<int64_t>& row_ids) {
        _read_by_rows = true;
        _row_ids = row_ids;
        return _set_read_one_line_impl();
    }

    /// The reader is responsible for counting the number of rows read,
    /// because some readers, such as parquet/orc,
    /// can skip some pages/rowgroups through indexes.
    virtual bool count_read_rows() { return false; }

protected:
    virtual Status _set_read_one_line_impl() {
        return Status::NotSupported("read_by_rows is not implemented for this reader.");
    }

    const size_t _MIN_BATCH_SIZE = 4064; // 4094 - 32(padding)

    /// Whether the underlying FileReader has filled the partition&missing columns
    bool _fill_all_columns = false;

    TPushAggOp::type _push_down_agg_type {};

    // For TopN queries, rows will be read according to row ids produced by TopN result.
    bool _read_by_rows = false;
    std::list<int64_t> _row_ids;

    // Cache to save some common part such as file footer.
    // Maybe null if not used
    FileMetaCache* _meta_cache = nullptr;

    // ---- Column descriptors (set by init_reader, owned by FileScanner) ----
    const std::vector<ColumnDescriptor>* _column_descs = nullptr;

    // ---- get_columns cache ----
    bool _get_columns_cached = false;
    std::unordered_map<std::string, DataTypePtr> _cached_name_to_type;
    const TQueryOptions _default_query_options;
};

/// Provides an accessor for the current batch's row positions within the file.
/// Implemented by RowGroupReader (Parquet) and OrcReader.
class RowPositionProvider {
public:
    virtual ~RowPositionProvider() = default;
    virtual const std::vector<segment_v2::rowid_t>& current_batch_row_positions() const = 0;
};

#include "common/compile_check_end.h"
} // namespace doris
