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

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vexpr_fwd.h"
#include "format/column_descriptor.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/segment/common.h"
#include "util/profile_collector.h"

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris {

class Block;
class VSlotRef;

// Context passed from FileScanner to readers for condition cache integration.
// On MISS: readers populate filter_result per-granule during predicate evaluation.
// On HIT: readers skip granules where filter_result[granule] == false.
struct ConditionCacheContext {
    bool is_hit = false;
    std::shared_ptr<std::vector<bool>> filter_result; // per-granule: true = has surviving rows
    int64_t base_granule = 0; // global granule index of the first granule in filter_result
    static constexpr int GRANULE_SIZE = 2048;
};

/// Base context for the unified init_reader(ReaderInitContext*) template method.
/// Contains fields shared by ALL reader types. Format-specific readers define
/// subclasses (ParquetInitContext, OrcInitContext, etc.) with extra fields.
/// FileScanner allocates the appropriate subclass and populates the shared fields
/// before calling init_reader().
struct ReaderInitContext {
    virtual ~ReaderInitContext() = default;

    // ---- Owned by FileScanner, shared by all readers ----
    std::vector<ColumnDescriptor>* column_descs = nullptr;
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
    TPushAggOp::type get_push_down_agg_type() const { return _push_down_agg_type; }

    /// Template method for reading blocks.
    /// Calls: on_before_read_block → _do_get_next_block → on_after_read_block
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) {
        RETURN_IF_ERROR(on_before_read_block(block));
        RETURN_IF_ERROR(_do_get_next_block(block, read_rows, eof));
        RETURN_IF_ERROR(on_after_read_block(block, read_rows));
        return Status::OK();
    }

    // Override this in readers that can adjust batch size between consecutive reads.
    virtual void set_batch_size(size_t batch_size) {}
    virtual size_t get_batch_size() const { return 0; }

    // Type is always nullable to process illegal values.
    // Results are cached after the first successful call.
    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type) {
        if (_get_columns_cached) {
            *name_to_type = _cached_name_to_type;
            return Status::OK();
        }
        RETURN_IF_ERROR(_get_columns_impl(name_to_type));
        _cached_name_to_type = *name_to_type;
        _get_columns_cached = true;

        return Status::OK();
    }

    virtual Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) {
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

    /// Returns true if on_before_init_reader has already set _column_descs.
    bool has_column_descs() const { return _column_descs != nullptr; }

    /// Unified initialization entry point (NVI pattern).
    /// Enforces the template method sequence for ALL readers:
    ///   _open_file_reader → on_before_init_reader → _do_init_reader → on_after_init_reader
    /// Subclasses implement _open_file_reader and _do_init_reader(ReaderInitContext*).
    /// FileScanner constructs the appropriate ReaderInitContext subclass and calls this.
    ///
    /// NOTE: During migration, readers not yet ported to this API still use their
    /// format-specific init_reader(...) methods. This method is non-virtual so it
    /// cannot be accidentally overridden.
    Status init_reader(ReaderInitContext* ctx) {
        // Apply push_down_agg_type early so _open_file_reader and _do_init_reader
        // can use it (e.g., PaimonCppReader skips full init on COUNT pushdown).
        // on_after_init_reader may reset this (e.g., Iceberg with equality deletes).
        set_push_down_agg_type(ctx->push_down_agg_type);

        RETURN_IF_ERROR(_open_file_reader(ctx));

        // Standalone readers (delete file readers, push handler) set column_descs=nullptr
        // and pre-populate column_names directly. Skip hooks for them.
        if (ctx->column_descs != nullptr) {
            RETURN_IF_ERROR(on_before_init_reader(ctx));
        }

        RETURN_IF_ERROR(_do_init_reader(ctx));

        if (ctx->column_descs != nullptr) {
            RETURN_IF_ERROR(on_after_init_reader(ctx));
        }

        return Status::OK();
    }

    /// Hook called before core init. Default just sets _column_descs.
    /// TableFormatReader overrides with partition/missing column computation.
    /// ORC/Parquet/Hive/Iceberg further override with format-specific schema matching.
    virtual Status on_before_init_reader(ReaderInitContext* ctx) {
        _column_descs = ctx->column_descs;
        return Status::OK();
    }

protected:
    // ---- Init-time hooks (Template Method for init_reader) ----

    /// Opens the file and prepares I/O resources before hooks run. Override in
    /// subclasses to open files, read metadata, set up decompressors, etc.
    /// For Parquet/ORC, opens the file and reads footer metadata.
    /// For CSV/JSON, opens the file, creates decompressors, and sets up line readers.
    /// Default is no-op (for JNI, Native, Arrow readers).
    virtual Status _open_file_reader(ReaderInitContext* /*ctx*/) { return Status::OK(); }

    /// Core initialization (format-specific). Subclasses override to perform
    /// their actual parsing engine setup. The context should be downcast to
    /// the appropriate subclass using checked_context_cast<T>.
    /// Default returns NotSupported — readers not yet migrated to the unified
    /// init_reader(ReaderInitContext*) API still use their old init methods.
    virtual Status _do_init_reader(ReaderInitContext* /*ctx*/) {
        return Status::NotSupported(
                "_do_init_reader(ReaderInitContext*) not yet implemented for this reader");
    }

    // ---- Existing init-time hooks ----

    /// Called after core init completes. Subclasses override to process
    /// delete files, deletion vectors, etc.
    virtual Status on_after_init_reader(ReaderInitContext* /*ctx*/) { return Status::OK(); }

    // ---- Read-time hooks ----

    /// Called before reading a block. Subclasses override to modify block
    /// structure (e.g. add ACID columns, expand for equality delete).
    virtual Status on_before_read_block(Block* block) { return Status::OK(); }

    /// Core block reading. Subclasses must override with actual read logic.
    virtual Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;

    /// Called after reading a block. Subclasses override to post-process
    /// (e.g. remove ACID columns, apply equality delete filter).
    virtual Status on_after_read_block(Block* block, size_t* read_rows) { return Status::OK(); }

    virtual Status _set_read_one_line_impl() {
        return Status::NotSupported("read_by_rows is not implemented for this reader.");
    }

    const size_t _MIN_BATCH_SIZE = 4064; // 4094 - 32(padding)

    // never let batch size be 0 because _do_get_next_block uses it as the
    // upper bound of a `while (block->rows() < batch_size)` loop and a 0 would make the reader
    // return without setting eof, causing the scanner to spin on empty blocks.
    const size_t _DEFAULT_BATCH_SIZE = 4064; // 4094 - 32(padding)
    TPushAggOp::type _push_down_agg_type {};

public:
    // Pass condition cache context to the reader for HIT/MISS tracking.
    virtual void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {}

    // Returns true if this reader can produce an accurate total row count from metadata
    // without reading actual data. Used to determine if CountReader decorator can be applied.
    // Only ORC and Parquet readers support this (via file footer metadata).
    virtual bool supports_count_pushdown() const { return false; }

    // Returns the total number of rows the reader will produce.
    // Used to pre-allocate condition cache with the correct number of granules.
    virtual int64_t get_total_rows() const { return 0; }

    // Returns true if this reader has delete operations (e.g. Iceberg position/equality deletes,
    // Hive ACID deletes). Used to disable condition cache when deletes are present, since cached
    // granule results may become stale if delete files change between queries.
    virtual bool has_delete_operations() const { return false; }

protected:
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
};

/// Provides an accessor for the current batch's row positions within the file.
/// Implemented by RowGroupReader (Parquet) and OrcReader.
class RowPositionProvider {
public:
    virtual ~RowPositionProvider() = default;
    virtual const std::vector<segment_v2::rowid_t>& current_batch_row_positions() const = 0;
};

} // namespace doris
