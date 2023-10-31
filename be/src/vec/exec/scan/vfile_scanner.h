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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "io/io_common.h"
#include "pipeline/exec/file_scan_operator.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {
class RuntimeState;
class TFileRangeDesc;
class TFileScanRange;
class TFileScanRangeParams;

namespace vectorized {
class ShardedKVCache;
class VExpr;
class VExprContext;
} // namespace vectorized
struct TypeDescriptor;
} // namespace doris

namespace doris::vectorized {

class NewFileScanNode;

class VFileScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(VFileScanner);

public:
    static constexpr const char* NAME = "VFileScanner";

    VFileScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                 const TFileScanRange& scan_range, RuntimeProfile* profile,
                 ShardedKVCache* kv_cache);

    VFileScanner(RuntimeState* state, pipeline::FileScanLocalState* parent, int64_t limit,
                 const TFileScanRange& scan_range, RuntimeProfile* profile,
                 ShardedKVCache* kv_cache);

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status prepare(const VExprContextSPtrs& conjuncts,
                   std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
                   const std::unordered_map<std::string, int>* colname_to_slot_id);

    std::string get_name() override { return VFileScanner::NAME; }

    std::string get_current_scan_range_name() override { return _current_range_path; }

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;

    Status _get_next_reader();

    // TODO: cast input block columns type to string.
    Status _cast_src_block(Block* block) { return Status::OK(); }

protected:
    const TFileScanRangeParams* _params;
    const std::vector<TFileRangeDesc>& _ranges;
    int _next_range;

    std::unique_ptr<GenericReader> _cur_reader;
    bool _cur_reader_eof;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    // File source slot descriptors
    std::vector<SlotDescriptor*> _file_slot_descs;
    // col names from _file_slot_descs
    std::vector<std::string> _file_col_names;
    // column id to name map. Collect from FE slot descriptor.
    std::unordered_map<int, std::string> _col_id_name_map;

    // Partition source slot descriptors
    std::vector<SlotDescriptor*> _partition_slot_descs;
    // Partition slot id to index in _partition_slot_descs
    std::unordered_map<SlotId, int> _partition_slot_index_map;
    // created from param.expr_of_dest_slot
    // For query, it saves default value expr of all dest columns, or nullptr for NULL.
    // For load, it saves conversion expr/default value of all dest columns.
    VExprContextSPtrs _dest_vexpr_ctx;
    // dest slot name to index in _dest_vexpr_ctx;
    std::unordered_map<std::string, int> _dest_slot_name_to_idx;
    // col name to default value expr
    std::unordered_map<std::string, vectorized::VExprContextSPtr> _col_default_value_ctx;
    // the map values of dest slot id to src slot desc
    // if there is not key of dest slot id in dest_sid_to_src_sid_without_trans, it will be set to nullptr
    std::vector<SlotDescriptor*> _src_slot_descs_order_by_dest;
    // dest slot desc index to src slot desc index
    std::unordered_map<int, int> _dest_slot_to_src_slot_index;

    std::unordered_map<std::string, size_t> _src_block_name_to_idx;

    // Get from GenericReader, save the existing columns in file to their type.
    std::unordered_map<std::string, TypeDescriptor> _name_to_col_type;
    // Get from GenericReader, save columns that required by scan but not exist in file.
    // These columns will be filled by default value or null.
    std::unordered_set<std::string> _missing_cols;

    //  The col names and types of source file, such as parquet, orc files.
    std::vector<std::string> _source_file_col_names;
    std::vector<TypeDescriptor> _source_file_col_types;
    std::map<std::string, TypeDescriptor*> _source_file_col_name_types;

    // For load task
    vectorized::VExprContextSPtrs _pre_conjunct_ctxs;
    std::unique_ptr<RowDescriptor> _src_row_desc;
    std::unique_ptr<RowDescriptor> _dest_row_desc;
    // row desc for default exprs
    std::unique_ptr<RowDescriptor> _default_val_row_desc;
    // owned by scan node
    ShardedKVCache* _kv_cache;

    bool _scanner_eof = false;
    int _rows = 0;
    int _num_of_columns_from_file;

    bool _src_block_mem_reuse = false;
    bool _strict_mode;

    bool _src_block_init = false;
    Block* _src_block_ptr;
    Block _src_block;

    VExprContextSPtrs _push_down_conjuncts;

    std::unique_ptr<io::FileCacheStatistics> _file_cache_statistics;
    std::unique_ptr<io::IOContext> _io_ctx;

    std::unique_ptr<std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>>
            _partition_columns;
    std::unique_ptr<std::unordered_map<std::string, VExprContextSPtr>> _missing_columns;

private:
    RuntimeProfile::Counter* _get_block_timer = nullptr;
    RuntimeProfile::Counter* _open_reader_timer = nullptr;
    RuntimeProfile::Counter* _cast_to_input_block_timer = nullptr;
    RuntimeProfile::Counter* _fill_path_columns_timer = nullptr;
    RuntimeProfile::Counter* _fill_missing_columns_timer = nullptr;
    RuntimeProfile::Counter* _pre_filter_timer = nullptr;
    RuntimeProfile::Counter* _convert_to_output_block_timer = nullptr;
    RuntimeProfile::Counter* _empty_file_counter = nullptr;
    RuntimeProfile::Counter* _file_counter = nullptr;

    const std::unordered_map<std::string, int>* _col_name_to_slot_id;
    // single slot filter conjuncts
    std::unordered_map<int, VExprContextSPtrs> _slot_id_to_filter_conjuncts;
    // not single(zero or multi) slot filter conjuncts
    VExprContextSPtrs _not_single_slot_filter_conjuncts;
    // save the path of current scan range
    std::string _current_range_path = "";

    // Only for load scan node.
    const TupleDescriptor* _input_tuple_desc = nullptr;
    // If _input_tuple_desc is set,
    // the _real_tuple_desc will point to _input_tuple_desc,
    // otherwise, point to _output_tuple_desc
    const TupleDescriptor* _real_tuple_desc = nullptr;

private:
    Status _init_expr_ctxes();
    Status _init_src_block(Block* block);
    Status _check_output_block_types();
    Status _cast_to_input_block(Block* block);
    Status _fill_columns_from_path(size_t rows);
    Status _fill_missing_columns(size_t rows);
    Status _pre_filter_src_block();
    Status _convert_to_output_block(Block* block);
    Status _truncate_char_or_varchar_columns(Block* block);
    void _truncate_char_or_varchar_column(Block* block, int idx, int len);
    Status _generate_fill_columns();
    Status _handle_dynamic_block(Block* block);
    Status _process_conjuncts_for_dict_filter();
    void _get_slot_ids(VExpr* expr, std::vector<int>* slot_ids);

    void _reset_counter() {
        _counter.num_rows_unselected = 0;
        _counter.num_rows_filtered = 0;
    }

    TPushAggOp::type _get_push_down_agg_type() {
        if (get_parent() != nullptr) {
            return _parent->get_push_down_agg_type();
        } else {
            return _local_state->get_push_down_agg_type();
        }
    }

    int64_t _get_push_down_count() {
        if (get_parent() != nullptr) {
            return _parent->get_push_down_count();
        } else {
            return _local_state->get_push_down_count();
        }
    }
};
} // namespace doris::vectorized
