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

#include "util/doris_metrics.h"

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/vtablet_sink.h"
#include "vec/core/block.h"

namespace doris {
namespace stream_load {

VOlapTableSink::VOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& texprs, Status* status)
        : OlapTableSink(pool, row_desc, texprs, status) {
    // From the thrift expressions create the real exprs.
    vectorized::VExpr::create_expr_trees(pool, texprs, &_output_vexpr_ctxs);
    // Do not use the origin data scala expr, clear scala expr contexts
    _output_expr_ctxs.clear();
    _name = "VOlapTableSink";
}

Status VOlapTableSink::init(const TDataSink& sink) {
    RETURN_IF_ERROR(OlapTableSink::init(sink));
    _vpartition = _pool->add(new VOlapTablePartitionParam(_schema, sink.olap_table_sink.partition));
    return _vpartition->init();
}

Status VOlapTableSink::prepare(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _input_row_desc, _expr_mem_tracker));
    return OlapTableSink::prepare(state);
}

Status VOlapTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    return OlapTableSink::open(state);
}

Status VOlapTableSink::send(RuntimeState* state, vectorized::Block* input_block) {
    Status status = Status::OK();
    if (UNLIKELY(input_block->rows() == 0)) { return status; }

    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += input_block->rows();
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(input_block->rows());
    state->update_num_bytes_load_total(input_block->bytes());
    DorisMetrics::instance()->load_rows->increment(input_block->rows());
    DorisMetrics::instance()->load_bytes->increment(input_block->bytes());

    vectorized::Block block(input_block->get_columns_with_type_and_name());
    if (!_output_vexpr_ctxs.empty()) {
        // Do vectorized expr here to speed up load
        block = vectorized::VExprContext::get_output_block_after_execute_exprs(
            _output_vexpr_ctxs, *input_block, status);
        if (UNLIKELY(block.rows() == 0)) { return status; }
    }

    auto num_rows = block.rows();
    int num_invalid_rows = 0;
    {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_vec.resize(num_rows);
        num_invalid_rows = _validate_data(state, &block, reinterpret_cast<bool*>(_filter_vec.data()));
        _number_filtered_rows += num_invalid_rows;
    }

    BlockRow block_row;
    SCOPED_RAW_TIMER(&_send_data_ns);
    for (int i = 0; i < num_rows; ++i) {
        if (num_invalid_rows > 0 && _filter_vec[i] != 0) {
            continue;
        }
        const VOlapTablePartition* partition = nullptr;
        uint32_t dist_hash = 0;
        block_row = {&block, i};
        if (!_vpartition->find_tablet(&block_row, &partition, &dist_hash)) {
            std::stringstream ss;
            ss << "no partition for this tuple. tuple="
               << block.dump_data(i, 1);
#if BE_TEST
            LOG(INFO) << ss.str();
#else
            state->append_error_msg_to_file("", ss.str());
#endif
            _number_filtered_rows++;
            continue;
        }
        _partition_ids.emplace(partition->id);
        uint32_t tablet_index = dist_hash % partition->num_buckets;
        for (int j = 0; j < partition->indexes.size(); ++j) {
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index];
            RETURN_IF_ERROR(_channels[j]->add_row(block_row, tablet_id));
            _number_output_rows++;
        }
    }
    return Status::OK();
}

Status VOlapTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) return _close_status;
    vectorized::VExpr::close(_output_vexpr_ctxs, state);
    return OlapTableSink::close(state, exec_status);
}

int VOlapTableSink::_validate_data(doris::RuntimeState* state, doris::vectorized::Block* block,
                                   bool* filter_map) {
    const auto num_rows = block->rows();
    // set all row is valid
    memset(filter_map, 0, num_rows * sizeof(bool));

    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        block->get_by_position(i).column = block->get_by_position(i).column->convert_to_full_column_if_const();
        const auto& column = block->get_by_position(i).column;

        if (desc->is_nullable() && desc->type() == TYPE_OBJECT) {
            const auto& null_map = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column)
                    ->get_null_map_data();
            for (int j = 0; j < num_rows; ++j) {
                if (!filter_map[j]) {
                    if (null_map[j]) {
                        state->append_error_msg_to_file("", std::string("null is not allowed for "
                                                                        "bitmap column, column_name: ") + desc->col_name() + ";");
                        filter_map[j] = true;
                    }
                }
            }
        } else {
            auto column_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column);
            auto& real_column_ptr = column_ptr == nullptr ? column : (column_ptr->get_nested_column_ptr());

            switch (desc->type().type) {
                case TYPE_CHAR:
                case TYPE_VARCHAR: {
                    const auto column_string = assert_cast<const vectorized::ColumnString *>(real_column_ptr.get());

                    for (int j = 0; j < num_rows; ++j) {
                        if (!filter_map[j]) {
                            auto str_val = column_string->get_data_at(j);
                            if (str_val.size > desc->type().len) {
                                state->append_error_msg_to_file("", fmt::format(
                                        "the length of input is too long than schema. "
                                        "column_name: {}; input_str: [{}] schema length: {}; actual length: {}; ",
                                        desc->col_name(), str_val.to_string(),
                                        desc->type().len, str_val.size));
                                filter_map[j] = true;
                            }
                        }
                    }
                    break;
                }
                    // TODO: Support TYPE_STRING in the future
//            case TYPE_STRING: {
//                StringValue* str_val = (StringValue*)slot;
//                if (str_val->len > desc->type().MAX_STRING_LENGTH) {
//                    ss << "the length of input is too long than schema. "
//                       << "column_name: " << desc->col_name() << "; "
//                       << "first 128 bytes of input_str: [" << std::string(str_val->ptr, 128)
//                       << "] "
//                       << "schema length: " << desc->type().MAX_STRING_LENGTH << "; "
//                       << "actual length: " << str_val->len << "; ";
//                    row_valid = false;
//                    continue;
//                }
//                break;
//            }
                case TYPE_DECIMALV2: {
                    auto column_decimal = const_cast<vectorized::ColumnDecimal
                            <vectorized::Decimal128> *>(assert_cast<const vectorized::ColumnDecimal
                            <vectorized::Decimal128> *>(real_column_ptr.get()));

                    for (int j = 0; j < num_rows; ++j) {
                        if (!filter_map[j]) {
                            auto dec_val = binary_cast<vectorized::Int128, DecimalV2Value>(
                                    column_decimal->get_data()[j]);
                            if (dec_val.greater_than_scale(desc->type().scale)) {
                                auto code = dec_val.round(&dec_val, desc->type().scale, HALF_UP);
                                column_decimal->get_data()[j] = binary_cast<DecimalV2Value, vectorized::Int128>(
                                        dec_val);

                                if (code != E_DEC_OK) {
                                    state->append_error_msg_to_file("", "round one decimal failed.value=" +
                                                                        dec_val.to_string());
                                    filter_map[j] = true;
                                }
                            }

                            if (dec_val > _max_decimalv2_val[i] || dec_val < _min_decimalv2_val[i]) {
                                state->append_error_msg_to_file("", fmt::format(
                                        "decimal value is not valid for definition, column={}, "
                                        "value={}, precision={}, scale= {};",
                                        desc->col_name(), dec_val.to_string(), desc->type().precision,
                                        desc->type().scale));
                                filter_map[j] = true;
                            }
                        }
                    }
                    break;
                }
                case TYPE_HLL: {
                    auto column_string = assert_cast<const vectorized::ColumnString *>(real_column_ptr.get());

                    for (int j = 0; j < num_rows; ++j) {
                        if (!filter_map[j]) {
                            auto str_val = column_string->get_data_at(j);
                            if (!HyperLogLog::is_valid(Slice(str_val.data, str_val.size))) {
                                state->append_error_msg_to_file("", std::string(
                                        "Content of HLL type column is invalid column_name: " + desc->col_name() +
                                        ";"));
                                filter_map[j] = true;
                            }
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            // Dispose the nullable column not match problem here, convert to nullable column
            if (desc->is_nullable() && !column_ptr) {
                block->get_by_position(i).column = vectorized::make_nullable(column);
                block->get_by_position(i).type = vectorized::make_nullable(block->get_by_position(i).type);
            }

            // Dispose the nullable column not match problem here, convert to not nullable column
            if (!desc->is_nullable() && column_ptr) {
                const auto& null_map = column_ptr->get_null_map_data();
                for (int j = 0; j < null_map.size(); ++j) {
                    if (null_map[j] && !filter_map[j]) {
                        filter_map[j] = true;
                        std::stringstream ss;
                        ss << "null value for not null column, column=" << desc->col_name();
#if BE_TEST
                        LOG(INFO) << ss.str();
#else
                        state->append_error_msg_to_file("", ss.str());
#endif
                    }
                }
                block->get_by_position(i).column = column_ptr->get_nested_column_ptr();
                block->get_by_position(i).type = (reinterpret_cast<const vectorized::DataTypeNullable*>(
                        block->get_by_position(i).type.get()))->get_nested_type();
            }
        }
    }

    auto filter_row = 0;
    for (int i = 0; i < num_rows; ++i) {
        filter_row += filter_map[i];
    }
    return filter_row;
}

} // namespace stream_load
} // namespace doris

