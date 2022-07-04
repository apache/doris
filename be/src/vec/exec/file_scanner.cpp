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

#include "file_scanner.h"

#include <fmt/format.h>

#include <vec/data_types/data_type_factory.hpp>

#include "common/logging.h"
#include "common/utils.h"
#include "exec/exec_node.h"
#include "exec/text_converter.hpp"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"

namespace doris::vectorized {

FileScanner::FileScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TFileScanRangeParams& params,
                         const std::vector<TFileRangeDesc>& ranges,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : _state(state),
          _params(params),
          _ranges(ranges),
          _next_range(0),
          _counter(counter),
#if BE_TEST
          _mem_tracker(new MemTracker()),
#else
          _mem_tracker(MemTracker::create_tracker(
                  -1, state->query_type() == TQueryType::LOAD
                              ? "FileScanner:" + std::to_string(state->load_job_id())
                              : "FileScanner:Select")),
#endif
          _mem_pool(std::make_unique<MemPool>(_mem_tracker.get())),
          _pre_filter_texprs(pre_filter_texprs),
          _profile(profile),
          _rows_read_counter(nullptr),
          _read_timer(nullptr),
          _scanner_eof(false) {
    _text_converter.reset(new (std::nothrow) TextConverter('\\'));
}

Status FileScanner::open() {
    RETURN_IF_ERROR(_init_expr_ctxes());

    _rows_read_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_profile, "TotalRawReadTime(*)");

    return Status::OK();
}

Status FileScanner::_init_expr_ctxes() {
    const TupleDescriptor* src_tuple_desc =
            _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status::InternalError(ss.str());
    }
    DCHECK(!_ranges.empty());

    std::map<SlotId, int> _full_src_index_map;
    std::map<SlotId, SlotDescriptor*> _full_src_slot_map;
    int index = 0;
    for (const auto& slot_desc : src_tuple_desc->slots()) {
        _full_src_slot_map.emplace(slot_desc->id(), slot_desc);
        _full_src_index_map.emplace(slot_desc->id(), index++);
    }

    _num_of_columns_from_file = _params.num_of_columns_from_file;
    for (const auto& slot_info : _params.required_slots) {
        auto slot_id = slot_info.slot_id;
        auto it = _full_src_slot_map.find(slot_id);
        if (it == std::end(_full_src_slot_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status::InternalError(ss.str());
        }
        _required_slot_descs.emplace_back(it->second);
        if (slot_info.is_file_slot) {
            _file_slot_descs.emplace_back(it->second);
            auto iti = _full_src_index_map.find(slot_id);
            _file_slot_index_map.emplace(slot_id, iti->second);
        } else {
            _partition_slot_descs.emplace_back(it->second);
            auto iti = _full_src_index_map.find(slot_id);
            _partition_slot_index_map.emplace(slot_id, iti->second - _num_of_columns_from_file);
        }
    }

    _row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                      std::vector<TupleId>({_params.src_tuple_id}),
                                      std::vector<bool>({false})));

    // preceding filter expr should be initialized by using `_row_desc`, which is the source row descriptor
    if (!_pre_filter_texprs.empty()) {
        // for vectorized, preceding filter exprs should be compounded to one passed from fe.
        DCHECK(_pre_filter_texprs.size() == 1);
        _vpre_filter_ctx_ptr.reset(new doris::vectorized::VExprContext*);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                _state->obj_pool(), _pre_filter_texprs[0], _vpre_filter_ctx_ptr.get()));
        RETURN_IF_ERROR((*_vpre_filter_ctx_ptr)->prepare(_state, *_row_desc, _mem_tracker));
        RETURN_IF_ERROR((*_vpre_filter_ctx_ptr)->open(_state));
    }

    return Status::OK();
}

void FileScanner::close() {
    if (_vpre_filter_ctx_ptr) {
        (*_vpre_filter_ctx_ptr)->close(_state);
    }
}

Status FileScanner::init_block(vectorized::Block* block) {
    (*block).clear();
    _rows = 0;
    for (const auto& slot_desc : _required_slot_descs) {
        if (slot_desc == nullptr) {
            continue;
        }
        auto is_nullable = slot_desc->is_nullable();
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(),
                                                                                  is_nullable);
        if (data_type == nullptr) {
            return Status::NotSupported(
                    fmt::format("Not support type for column:{}", slot_desc->col_name()));
        }
        MutableColumnPtr data_column = data_type->create_column();
        (*block).insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    return Status::OK();
}

Status FileScanner::_filter_block(vectorized::Block* _block) {
    auto origin_column_num = (*_block).columns();
    // filter block
    auto old_rows = (*_block).rows();
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_vpre_filter_ctx_ptr, _block,
                                                           origin_column_num));
    _counter->num_rows_unselected += old_rows - (*_block).rows();
    return Status::OK();
}

Status FileScanner::finalize_block(vectorized::Block* _block, bool* eof) {
    *eof = _scanner_eof;
    RETURN_IF_ERROR(_fill_columns_from_path(_block));
    if (LIKELY(_rows > 0)) {
        RETURN_IF_ERROR(_filter_block(_block));
    }

    return Status::OK();
}

Status FileScanner::_fill_columns_from_path(vectorized::Block* _block) {
    const TFileRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.columns_from_path && !_partition_slot_descs.empty()) {
        size_t rows = _rows;

        for (const auto& slot_desc : _partition_slot_descs) {
            if (slot_desc == nullptr) continue;
            auto it = _partition_slot_index_map.find(slot_desc->id());
            if (it == std::end(_partition_slot_index_map)) {
                std::stringstream ss;
                ss << "Unknown source slot descriptor, slot_id=" << slot_desc->id();
                return Status::InternalError(ss.str());
            }
            const std::string& column_from_path = range.columns_from_path[it->second];

            auto doris_column = _block->get_by_name(slot_desc->col_name()).column;
            IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());

            for (size_t j = 0; j < rows; ++j) {
                _text_converter->write_vec_column(slot_desc, col_ptr,
                                                  const_cast<char*>(column_from_path.c_str()),
                                                  column_from_path.size(), true, false);
            }
        }
    }
    return Status::OK();
}

} // namespace doris::vectorized
