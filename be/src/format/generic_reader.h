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

// This a reader interface for all file readers.
// A GenericReader is responsible for reading a file and return
// a set of blocks with specified schema,
class GenericReader : public ProfileCollector {
public:
    GenericReader() : _push_down_agg_type(TPushAggOp::type::NONE) {}
    void set_push_down_agg_type(TPushAggOp::type push_down_agg_type) {
        _push_down_agg_type = push_down_agg_type;
    }

    virtual Status get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;

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
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
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

public:
    // Pass condition cache context to the reader for HIT/MISS tracking.
    virtual void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {}

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
};

} // namespace doris
