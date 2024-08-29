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

#include "vec/exprs/table_function/vexplode_json_object.h"

#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VExplodeJsonObjectTableFunction::VExplodeJsonObjectTableFunction() {
    _fn_name = "vexplode_json_object";
}

Status VExplodeJsonObjectTableFunction::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << "VExplodeJsonObjectTableFunction only support 1 child but has "
            << _expr_context->root()->children().size();

    int text_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &text_column_idx));

    _json_object_column = block->get_by_position(text_column_idx).column;
    return Status::OK();
}

void VExplodeJsonObjectTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    StringRef text = _json_object_column->get_data_at(row_idx);
    if (text.data != nullptr) {
        JsonbDocument* doc = JsonbDocument::createDocument(text.data, text.size);
        if (UNLIKELY(!doc || !doc->getValue())) {
            // error jsonb, put null into output, cur_size = 0 , we will insert_default
            return;
        }
        // value is NOT necessary to be deleted since JsonbValue will not allocate memory
        JsonbValue* value = doc->getValue();
        auto writer = std::make_unique<JsonbWriter>();
        if (value->isObject()) {
            _cur_size = value->length();
            ObjectVal* obj = (ObjectVal*)value;
            _object_pairs.first =
                    ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
            _object_pairs.second =
                    ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
            _object_pairs.first->reserve(_cur_size);
            _object_pairs.second->reserve(_cur_size);
            for (auto it = obj->begin(); it != obj->end(); ++it) {
                _object_pairs.first->insert_data(it->getKeyStr(), it->klen());
                writer->reset();
                writer->writeValue(it->value());
                if (it->value()->isNull()) {
                    _object_pairs.second->insert_default();
                } else {
                    const std::string_view& jsonb_value = std::string_view(
                            writer->getOutput()->getBuffer(), writer->getOutput()->getSize());
                    _object_pairs.second->insert_data(jsonb_value.data(), jsonb_value.size());
                }
            }
        }
        // we do not support other json type except object
    }
}

void VExplodeJsonObjectTableFunction::process_close() {
    _json_object_column = nullptr;
    _object_pairs.first = nullptr;
    _object_pairs.second = nullptr;
}

void VExplodeJsonObjectTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    // if current is empty map row, also append a default value
    if (current_empty()) {
        column->insert_many_defaults(length);
        return;
    }
    ColumnStruct* ret = nullptr;
    // this _is_nullable is whole output column's nullable
    if (_is_nullable) {
        // make map kv value into struct
        ret = assert_cast<ColumnStruct*>(
                assert_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get());
        assert_cast<ColumnUInt8*>(
                assert_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                ->insert_many_defaults(length);
    } else if (column->is_column_struct()) {
        ret = assert_cast<ColumnStruct*>(column.get());
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "only support expand json object int to struct(kv pair), but given: ",
                        column->get_name());
    }
    if (!ret || ret->tuple_size() != 2) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "only support expand json object int to kv pair column, but given: ",
                        ret->tuple_size());
    }
    ret->get_column(0).insert_many_from(*_object_pairs.first, _cur_offset, length);
    ret->get_column(1).insert_many_from(*_object_pairs.second, _cur_offset, length);
}

int VExplodeJsonObjectTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        ColumnStruct* struct_column = nullptr;
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            struct_column =
                    assert_cast<ColumnStruct*>(nullable_column->get_nested_column_ptr().get());
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            // here nullmap_column insert max_step many defaults as if MAP[row_idx] is NULL
            // will be not update value, _cur_size = 0, means current_empty;
            // so here could insert directly
            nullmap_column->insert_many_defaults(max_step);
        } else {
            struct_column = assert_cast<ColumnStruct*>(column.get());
        }
        if (!struct_column || struct_column->tuple_size() != 2) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "only support expand json object int to kv pair column, but given:  ",
                            struct_column->tuple_size());
        }
        struct_column->get_column(0).insert_range_from(*_object_pairs.first, _cur_offset, max_step);
        struct_column->get_column(1).insert_range_from(*_object_pairs.second, _cur_offset,
                                                       max_step);
    }
    forward(max_step);
    return max_step;
}
} // namespace doris::vectorized
