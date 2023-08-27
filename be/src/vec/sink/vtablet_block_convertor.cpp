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

#include "vec/sink/vtablet_block_convertor.h"

#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <google/protobuf/stubs/common.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/binary_cast.hpp"
#include "util/brpc_client_cache.h"
#include "util/thread.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace stream_load {

Status OlapTableBlockConvertor::validate_and_convert_block(
        RuntimeState* state, vectorized::Block* input_block,
        std::shared_ptr<vectorized::Block>& block, vectorized::VExprContextSPtrs output_vexpr_ctxs,
        size_t rows, bool eos, bool& has_filtered_rows) {
    DCHECK(input_block->rows() > 0);

    block = vectorized::Block::create_shared(input_block->get_columns_with_type_and_name());
    if (!output_vexpr_ctxs.empty()) {
        // Do vectorized expr here to speed up load
        RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
                output_vexpr_ctxs, *input_block, block.get()));
    }

    // fill the valus for auto-increment columns
    if (_auto_inc_col_idx.has_value()) {
        RETURN_IF_ERROR(_fill_auto_inc_cols(block.get(), rows, eos));
    }

    int64_t filtered_rows = 0;
    {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(block->rows());
        bool stop_processing = false;
        RETURN_IF_ERROR(_validate_data(state, block.get(), filtered_rows, &stop_processing));
        _num_filtered_rows += filtered_rows;
        has_filtered_rows = filtered_rows > 0;
        if (stop_processing) {
            // should be returned after updating "_number_filtered_rows", to make sure that load job can be cancelled
            // because of "data unqualified"
            return Status::EndOfFile("Encountered unqualified data, stop processing");
        }
        _convert_to_dest_desc_block(block.get());
    }

    return Status::OK();
}

void OlapTableBlockConvertor::init_autoinc_info(int64_t db_id, int64_t table_id, int batch_size) {
    _batch_size = batch_size;
    for (size_t idx = 0; idx < _output_tuple_desc->slots().size(); idx++) {
        if (_output_tuple_desc->slots()[idx]->is_auto_increment()) {
            _auto_inc_col_idx = idx;
            _auto_inc_id_buffer = GlobalAutoIncBuffers::GetInstance()->get_auto_inc_buffer(
                    db_id, table_id, _output_tuple_desc->slots()[idx]->col_unique_id());
            _auto_inc_id_buffer->set_batch_size_at_least(_batch_size);
            break;
        }
    }
}

template <bool is_min>
DecimalV2Value OlapTableBlockConvertor::_get_decimalv2_min_or_max(const TypeDescriptor& type) {
    std::map<std::pair<int, int>, DecimalV2Value>* pmap;
    if constexpr (is_min) {
        pmap = &_min_decimalv2_val;
    } else {
        pmap = &_max_decimalv2_val;
    }

    // found
    auto iter = pmap->find({type.precision, type.scale});
    if (iter != pmap->end()) {
        return iter->second;
    }

    // save min or max DecimalV2Value for next time
    DecimalV2Value value;
    if constexpr (is_min) {
        value.to_min_decimal(type.precision, type.scale);
    } else {
        value.to_max_decimal(type.precision, type.scale);
    }
    pmap->emplace(std::pair<int, int> {type.precision, type.scale}, value);
    return value;
}

Status OlapTableBlockConvertor::_validate_column(RuntimeState* state, const TypeDescriptor& type,
                                                 bool is_nullable, vectorized::ColumnPtr column,
                                                 size_t slot_index, bool* stop_processing,
                                                 fmt::memory_buffer& error_prefix,
                                                 vectorized::IColumn::Permutation* rows) {
    DCHECK((rows == nullptr) || (rows->size() == column->size()));
    fmt::memory_buffer error_msg;
    auto set_invalid_and_append_error_msg = [&](int row) {
        _filter_bitmap.Set(row, true);
        auto ret = state->append_error_msg_to_file([]() -> std::string { return ""; },
                                                   [&error_prefix, &error_msg]() -> std::string {
                                                       return fmt::to_string(error_prefix) +
                                                              fmt::to_string(error_msg);
                                                   },
                                                   stop_processing);
        error_msg.clear();
        return ret;
    };

    auto column_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column);
    auto& real_column_ptr = column_ptr == nullptr ? column : (column_ptr->get_nested_column_ptr());
    auto null_map = column_ptr == nullptr ? nullptr : column_ptr->get_null_map_data().data();
    auto need_to_validate = [&null_map, this](size_t j, size_t row) {
        return !_filter_bitmap.Get(row) && (null_map == nullptr || null_map[j] == 0);
    };

    ssize_t last_invalid_row = -1;
    switch (type.type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const auto column_string =
                assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());

        size_t limit = config::string_type_length_soft_limit_bytes;
        // when type.len is negative, std::min will return overflow value, so we need to check it
        if (type.len > 0) {
            limit = std::min(config::string_type_length_soft_limit_bytes, type.len);
        }
        for (size_t j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (need_to_validate(j, row)) {
                auto str_val = column_string->get_data_at(j);
                bool invalid = str_val.size > limit;
                if (invalid) {
                    last_invalid_row = row;
                    if (str_val.size > type.len) {
                        fmt::format_to(error_msg, "{}",
                                       "the length of input is too long than schema. ");
                        fmt::format_to(error_msg, "first 32 bytes of input str: [{}] ",
                                       str_val.to_prefix(32));
                        fmt::format_to(error_msg, "schema length: {}; ", type.len);
                        fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                    } else if (str_val.size > limit) {
                        fmt::format_to(error_msg, "{}",
                                       "the length of input string is too long than vec schema. ");
                        fmt::format_to(error_msg, "first 32 bytes of input str: [{}] ",
                                       str_val.to_prefix(32));
                        fmt::format_to(error_msg, "schema length: {}; ", type.len);
                        fmt::format_to(error_msg, "limit length: {}; ", limit);
                        fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                    }
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
                }
            }
        }
        break;
    }
    case TYPE_JSONB: {
        const auto column_string =
                assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());
        for (size_t j = 0; j < column->size(); ++j) {
            if (!_filter_bitmap.Get(j)) {
                if (is_nullable && column_ptr && column_ptr->is_null_at(j)) {
                    continue;
                }
                auto str_val = column_string->get_data_at(j);
                bool invalid = str_val.size == 0;
                if (invalid) {
                    error_msg.clear();
                    fmt::format_to(error_msg, "{}", "jsonb with size 0 is invalid");
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                }
            }
        }
        break;
    }
    case TYPE_DECIMALV2: {
        auto column_decimal = const_cast<vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                        real_column_ptr.get()));
        const auto& max_decimalv2 = _get_decimalv2_min_or_max<false>(type);
        const auto& min_decimalv2 = _get_decimalv2_min_or_max<true>(type);
        for (size_t j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (need_to_validate(j, row)) {
                auto dec_val = binary_cast<vectorized::Int128, DecimalV2Value>(
                        column_decimal->get_data()[j]);
                bool invalid = false;

                if (dec_val.greater_than_scale(type.scale)) {
                    auto code = dec_val.round(&dec_val, type.scale, HALF_UP);
                    column_decimal->get_data()[j] = dec_val.value();

                    if (code != E_DEC_OK) {
                        fmt::format_to(error_msg, "round one decimal failed.value={}; ",
                                       dec_val.to_string());
                        invalid = true;
                    }
                }
                if (dec_val > max_decimalv2 || dec_val < min_decimalv2) {
                    fmt::format_to(error_msg, "{}", "decimal value is not valid for definition");
                    fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                    fmt::format_to(error_msg, ", precision={}, scale={}", type.precision,
                                   type.scale);
                    fmt::format_to(error_msg, ", min={}, max={}; ", min_decimalv2.to_string(),
                                   max_decimalv2.to_string());
                    invalid = true;
                }

                if (invalid) {
                    last_invalid_row = row;
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
                }
            }
        }
        break;
    }
    case TYPE_DECIMAL32: {
#define CHECK_VALIDATION_FOR_DECIMALV3(DecimalType)                                                \
    auto column_decimal = const_cast<vectorized::ColumnDecimal<DecimalType>*>(                     \
            assert_cast<const vectorized::ColumnDecimal<DecimalType>*>(real_column_ptr.get()));    \
    const auto& max_decimal = type_limit<DecimalType>::max();                                      \
    const auto& min_decimal = type_limit<DecimalType>::min();                                      \
    for (size_t j = 0; j < column->size(); ++j) {                                                  \
        auto row = rows ? (*rows)[j] : j;                                                          \
        if (row == last_invalid_row) {                                                             \
            continue;                                                                              \
        }                                                                                          \
        if (need_to_validate(j, row)) {                                                            \
            auto dec_val = column_decimal->get_data()[j];                                          \
            bool invalid = false;                                                                  \
            if (dec_val > max_decimal || dec_val < min_decimal) {                                  \
                fmt::format_to(error_msg, "{}", "decimal value is not valid for definition");      \
                fmt::format_to(error_msg, ", value={}", dec_val);                                  \
                fmt::format_to(error_msg, ", precision={}, scale={}", type.precision, type.scale); \
                fmt::format_to(error_msg, ", min={}, max={}; ", min_decimal, max_decimal);         \
                invalid = true;                                                                    \
            }                                                                                      \
            if (invalid) {                                                                         \
                last_invalid_row = row;                                                            \
                RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));                            \
            }                                                                                      \
        }                                                                                          \
    }
        CHECK_VALIDATION_FOR_DECIMALV3(vectorized::Decimal32);
        break;
    }
    case TYPE_DECIMAL64: {
        CHECK_VALIDATION_FOR_DECIMALV3(vectorized::Decimal64);
        break;
    }
    case TYPE_DECIMAL128I: {
        CHECK_VALIDATION_FOR_DECIMALV3(vectorized::Decimal128I);
        break;
    }
#undef CHECK_VALIDATION_FOR_DECIMALV3
    case TYPE_ARRAY: {
        const auto column_array =
                assert_cast<const vectorized::ColumnArray*>(real_column_ptr.get());
        DCHECK(type.children.size() == 1);
        auto nested_type = type.children[0];
        const auto& offsets = column_array->get_offsets();
        vectorized::IColumn::Permutation permutation(offsets.back());
        for (size_t r = 0; r < offsets.size(); ++r) {
            for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                permutation[c] = rows ? (*rows)[r] : r;
            }
        }
        fmt::format_to(error_prefix, "ARRAY type failed: ");
        RETURN_IF_ERROR(_validate_column(state, nested_type, type.contains_nulls[0],
                                         column_array->get_data_ptr(), slot_index, stop_processing,
                                         error_prefix, &permutation));
        break;
    }
    case TYPE_MAP: {
        const auto column_map = assert_cast<const vectorized::ColumnMap*>(real_column_ptr.get());
        DCHECK(type.children.size() == 2);
        auto key_type = type.children[0];
        auto val_type = type.children[1];
        const auto& offsets = column_map->get_offsets();
        vectorized::IColumn::Permutation permutation(offsets.back());
        for (size_t r = 0; r < offsets.size(); ++r) {
            for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                permutation[c] = rows ? (*rows)[r] : r;
            }
        }
        fmt::format_to(error_prefix, "MAP type failed: ");
        RETURN_IF_ERROR(_validate_column(state, key_type, type.contains_nulls[0],
                                         column_map->get_keys_ptr(), slot_index, stop_processing,
                                         error_prefix, &permutation));
        RETURN_IF_ERROR(_validate_column(state, val_type, type.contains_nulls[1],
                                         column_map->get_values_ptr(), slot_index, stop_processing,
                                         error_prefix, &permutation));
        break;
    }
    case TYPE_STRUCT: {
        const auto column_struct =
                assert_cast<const vectorized::ColumnStruct*>(real_column_ptr.get());
        DCHECK(type.children.size() == column_struct->tuple_size());
        fmt::format_to(error_prefix, "STRUCT type failed: ");
        for (size_t sc = 0; sc < column_struct->tuple_size(); ++sc) {
            RETURN_IF_ERROR(_validate_column(state, type.children[sc], type.contains_nulls[sc],
                                             column_struct->get_column_ptr(sc), slot_index,
                                             stop_processing, error_prefix));
        }
        break;
    }
    default:
        break;
    }

    // Dispose the column should do not contain the NULL value
    // Only two case:
    // 1. column is nullable but the desc is not nullable
    // 2. desc->type is BITMAP
    if ((!is_nullable || type == TYPE_OBJECT) && column_ptr) {
        for (int j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (null_map[j] && !_filter_bitmap.Get(row)) {
                fmt::format_to(error_msg, "null value for not null column, type={}",
                               type.debug_string());
                last_invalid_row = row;
                RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
            }
        }
    }

    return Status::OK();
}

Status OlapTableBlockConvertor::_validate_data(RuntimeState* state, vectorized::Block* block,
                                               int64_t& filtered_rows, bool* stop_processing) {
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        block->get_by_position(i).column =
                block->get_by_position(i).column->convert_to_full_column_if_const();
        const auto& column = block->get_by_position(i).column;

        fmt::memory_buffer error_prefix;
        fmt::format_to(error_prefix, "column_name[{}], ", desc->col_name());
        RETURN_IF_ERROR(_validate_column(state, desc->type(), desc->is_nullable(), column, i,
                                         stop_processing, error_prefix));
    }

    filtered_rows = 0;
    for (int i = 0; i < block->rows(); ++i) {
        filtered_rows += _filter_bitmap.Get(i);
    }
    return Status::OK();
}

void OlapTableBlockConvertor::_convert_to_dest_desc_block(doris::vectorized::Block* block) {
    for (int i = 0; i < _output_tuple_desc->slots().size() && i < block->columns(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        if (desc->is_nullable() != block->get_by_position(i).type->is_nullable()) {
            if (desc->is_nullable()) {
                block->get_by_position(i).type =
                        vectorized::make_nullable(block->get_by_position(i).type);
                block->get_by_position(i).column =
                        vectorized::make_nullable(block->get_by_position(i).column);
            } else {
                block->get_by_position(i).type = assert_cast<const vectorized::DataTypeNullable&>(
                                                         *block->get_by_position(i).type)
                                                         .get_nested_type();
                block->get_by_position(i).column = assert_cast<const vectorized::ColumnNullable&>(
                                                           *block->get_by_position(i).column)
                                                           .get_nested_column_ptr();
            }
        }
    }
}

Status OlapTableBlockConvertor::_fill_auto_inc_cols(vectorized::Block* block, size_t rows,
                                                    bool eos) {
    size_t idx = _auto_inc_col_idx.value();
    SlotDescriptor* slot = _output_tuple_desc->slots()[idx];
    DCHECK(slot->type().type == PrimitiveType::TYPE_BIGINT);
    DCHECK(!slot->is_nullable());

    size_t null_value_count = 0;
    auto dst_column = vectorized::ColumnInt64::create();
    vectorized::ColumnInt64::Container& dst_values = dst_column->get_data();

    vectorized::ColumnPtr src_column_ptr = block->get_by_position(idx).column;
    DCHECK(vectorized::is_column_const(*src_column_ptr) || src_column_ptr->is_nullable());
    if (const vectorized::ColumnConst* const_column =
                check_and_get_column<vectorized::ColumnConst>(src_column_ptr)) {
        // for insert stmt like "insert into tbl1 select null,col1,col2,... from tbl2" or
        // "insert into tbl1 select 1,col1,col2,... from tbl2", the type of literal's column
        // will be `ColumnConst`
        if (const_column->is_null_at(0)) {
            // the input of autoinc column are all null literals
            // fill the column with generated ids
            null_value_count = rows;
            std::vector<std::pair<int64_t, size_t>> res;
            RETURN_IF_ERROR(_auto_inc_id_buffer->sync_request_ids(null_value_count, &res));
            for (auto [start, length] : res) {
                _auto_inc_id_allocator.insert_ids(start, length);
            }

            for (size_t i = 0; i < rows; i++) {
                dst_values.emplace_back(_auto_inc_id_allocator.next_id());
            }
        } else {
            // the input of autoinc column are all int64 literals
            // fill the column with that literal
            int64_t value = const_column->get_int(0);
            dst_values.resize_fill(rows, value);
        }
    } else {
        const auto& src_nullable_column =
                assert_cast<const vectorized::ColumnNullable&>(*src_column_ptr);
        auto src_nested_column_ptr = src_nullable_column.get_nested_column_ptr();
        const auto& null_map_data = src_nullable_column.get_null_map_data();
        dst_values.reserve(rows);
        for (size_t i = 0; i < rows; i++) {
            null_value_count += null_map_data[i];
        }
        std::vector<std::pair<int64_t, size_t>> res;
        RETURN_IF_ERROR(_auto_inc_id_buffer->sync_request_ids(null_value_count, &res));
        for (auto [start, length] : res) {
            _auto_inc_id_allocator.insert_ids(start, length);
        }

        for (size_t i = 0; i < rows; i++) {
            dst_values.emplace_back((null_map_data[i] != 0) ? _auto_inc_id_allocator.next_id()
                                                            : src_nested_column_ptr->get_int(i));
        }
    }
    block->get_by_position(idx).column = std::move(dst_column);
    block->get_by_position(idx).type =
            vectorized::DataTypeFactory::instance().create_data_type(slot->type(), false);
    return Status::OK();
}

} // namespace stream_load
} // namespace doris