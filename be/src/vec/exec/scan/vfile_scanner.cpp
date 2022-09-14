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

#include "vec/exec/scan/vfile_scanner.h"

#include <fmt/format.h>

#include <vec/data_types/data_type_factory.hpp>

#include "common/logging.h"
#include "common/utils.h"
#include "exec/exec_node.h"
#include "exec/text_converter.hpp"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "vec/exec/scan/new_file_scan_node.h"

namespace doris::vectorized {

VFileScanner::VFileScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                           const TFileScanRange& scan_range, MemTracker* tracker,
                           RuntimeProfile* profile, const std::vector<TExpr>& pre_filter_texprs,
                           TFileFormatType::type format)
        : VScanner(state, static_cast<VScanNode*>(parent), limit, tracker),
          _params(scan_range.params),
          _ranges(scan_range.ranges),
          _next_range(0),
          _file_format(format),
          _mem_pool(std::make_unique<MemPool>()),
          _profile(profile),
          _pre_filter_texprs(pre_filter_texprs),
          _strict_mode(false) {}

Status VFileScanner::prepare(VExprContext** vconjunct_ctx_ptr) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);

    if (vconjunct_ctx_ptr != nullptr) {
        // Copy vconjunct_ctx_ptr from scan node to this scanner's _vconjunct_ctx.
        RETURN_IF_ERROR((*vconjunct_ctx_ptr)->clone(_state, &_vconjunct_ctx));
    }

    return Status::OK();
}

Status VFileScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::open(state));
    RETURN_IF_ERROR(_init_expr_ctxes());
    return Status::OK();
}

Status VFileScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    if (_cur_reader == nullptr || _cur_reader_eof) {
        _get_next_reader();
    }
    if (!_scanner_eof) {
        _cur_reader->get_next_block(block, &_cur_reader_eof);
    }

    if (block->rows() > 0) {
        _fill_columns_from_path(block, block->rows());
        // TODO: cast to String for load job.
    }

    if (_scanner_eof && block->rows() == 0) {
        *eof = true;
    }
    return Status::OK();
}

Status VFileScanner::_fill_columns_from_path(vectorized::Block* _block, size_t rows) {
    const TFileRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.columns_from_path && !_partition_slot_descs.empty()) {
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

Status VFileScanner::_get_next_reader() {
    //TODO: delete _cur_reader?
    while (true) {
        if (_next_range >= _ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        const TFileRangeDesc& range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;

        RETURN_IF_ERROR(FileFactory::create_file_reader(_state->exec_env(), _profile, _params,
                                                        range, file_reader));
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }
        _cur_reader.reset(new ParquetReader(
                file_reader.release(), _file_slot_descs.size(), _state->query_options().batch_size,
                range.start_offset, range.size, const_cast<cctz::time_zone*>(&_state->timezone_obj())));
        Status status =
                _cur_reader->init_reader(_output_tuple_desc, _file_slot_descs, _conjunct_ctxs, _state->timezone());
        return status;
    }
}

Status VFileScanner::_init_expr_ctxes() {
    if (_input_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status::InternalError(ss.str());
    }
    DCHECK(!_ranges.empty());

    std::map<SlotId, int> _full_src_index_map;
    std::map<SlotId, SlotDescriptor*> _full_src_slot_map;
    int index = 0;
    for (const auto& slot_desc : _input_tuple_desc->slots()) {
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

    _src_tuple = (doris::Tuple*)_mem_pool->allocate(_input_tuple_desc->byte_size());
    _src_tuple_row = (TupleRow*)_mem_pool->allocate(sizeof(Tuple*));
    _src_tuple_row->set_tuple(0, _src_tuple);
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
        RETURN_IF_ERROR((*_vpre_filter_ctx_ptr)->prepare(_state, *_row_desc));
        RETURN_IF_ERROR((*_vpre_filter_ctx_ptr)->open(_state));
    }

    // Construct dest slots information
    if (config::enable_new_load_scan_node) {
        if (_output_tuple_desc == nullptr) {
            return Status::InternalError("Unknown dest tuple descriptor, tuple_id={}",
                                         _params.dest_tuple_id);
        }

        bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
        for (auto slot_desc : _output_tuple_desc->slots()) {
            if (!slot_desc->is_materialized()) {
                continue;
            }
            auto it = _params.expr_of_dest_slot.find(slot_desc->id());
            if (it == std::end(_params.expr_of_dest_slot)) {
                return Status::InternalError("No expr for dest slot, id={}, name={}",
                                             slot_desc->id(), slot_desc->col_name());
            }

            vectorized::VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
            RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get()));
            RETURN_IF_ERROR(ctx->open(_state));
            _dest_vexpr_ctx.emplace_back(ctx);
            if (has_slot_id_map) {
                auto it1 = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
                if (it1 == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                    _src_slot_descs_order_by_dest.emplace_back(nullptr);
                } else {
                    auto _src_slot_it = _full_src_slot_map.find(it1->second);
                    if (_src_slot_it == std::end(_full_src_slot_map)) {
                        return Status::InternalError("No src slot {} in src slot descs",
                                                     it1->second);
                    }
                    _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
                }
            }
        }
    }

    return Status::OK();
}

} // namespace doris::vectorized
