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

#include "exprs/table_function/vjson_each.h"

#include <glog/logging.h>

#include <ostream>
#include <string>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_struct.h"
#include "core/string_ref.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "util/jsonb_document.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"

namespace doris {

template <bool TEXT_MODE>
VJsonEachTableFunction<TEXT_MODE>::VJsonEachTableFunction() {
    _fn_name = TEXT_MODE ? "vjson_each_text" : "vjson_each";
}

template <bool TEXT_MODE>
Status VJsonEachTableFunction<TEXT_MODE>::process_init(Block* block, RuntimeState* /*state*/) {
    ColumnPtr value_column;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute_column(
            _expr_context.get(), block, nullptr, block->rows(), value_column));
    auto [col, is_const] = unpack_if_const(value_column);
    _json_column = col;
    _is_const = is_const;
    return Status::OK();
}

// Helper: insert one JsonbValue as plain text into a ColumnNullable<ColumnString>.
// For strings: raw blob content (quotes stripped, matching json_each_text PG semantics).
// For null JSON values: SQL NULL (insert_default).
// For all others (numbers, bools, objects, arrays): JSON text representation.
static void insert_value_as_text(const JsonbValue* value, MutableColumnPtr& col) {
    if (value == nullptr || value->isNull()) {
        col->insert_default();
        return;
    }
    if (value->isString()) {
        const auto* str_val = value->unpack<JsonbStringVal>();
        col->insert_data(str_val->getBlob(), str_val->getBlobLen());
    } else {
        JsonbToJson converter;
        std::string text = converter.to_json_string(value);
        col->insert_data(text.data(), text.size());
    }
}

// Helper: insert one JsonbValue in JSONB binary form into a ColumnNullable<ColumnString>.
// For null JSON values: SQL NULL (insert_default).
// For all others: write JSONB binary via JsonbWriter.
static void insert_value_as_json(const JsonbValue* value, MutableColumnPtr& col,
                                 JsonbWriter& writer) {
    if (value == nullptr || value->isNull()) {
        col->insert_default();
        return;
    }
    writer.reset();
    writer.writeValue(value);
    const auto* buf = writer.getOutput()->getBuffer();
    size_t len = writer.getOutput()->getSize();
    col->insert_data(buf, len);
}

template <bool TEXT_MODE>
void VJsonEachTableFunction<TEXT_MODE>::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);
    if (_is_const && _cur_size > 0) {
        return;
    }

    StringRef text;
    const size_t idx = _is_const ? 0 : row_idx;
    if (const auto* nullable_col = check_and_get_column<ColumnNullable>(*_json_column)) {
        if (nullable_col->is_null_at(idx)) {
            return;
        }
        text = assert_cast<const ColumnString&>(nullable_col->get_nested_column()).get_data_at(idx);
    } else {
        text = assert_cast<const ColumnString&>(*_json_column).get_data_at(idx);
    }

    const JsonbDocument* doc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(text.data, text.size, &doc);
    if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
        return;
    }

    const JsonbValue* jv = doc->getValue();
    if (!jv->isObject()) {
        return;
    }

    const auto* obj = jv->unpack<ObjectVal>();
    _cur_size = obj->numElem();
    if (_cur_size == 0) {
        return;
    }

    _kv_pairs.first = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    _kv_pairs.second = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    _kv_pairs.first->reserve(_cur_size);
    _kv_pairs.second->reserve(_cur_size);

    if constexpr (TEXT_MODE) {
        for (const auto& kv : *obj) {
            _kv_pairs.first->insert_data(kv.getKeyStr(), kv.klen());
            insert_value_as_text(kv.value(), _kv_pairs.second);
        }
    } else {
        JsonbWriter writer;
        for (const auto& kv : *obj) {
            _kv_pairs.first->insert_data(kv.getKeyStr(), kv.klen());
            insert_value_as_json(kv.value(), _kv_pairs.second, writer);
        }
    }
}

template <bool TEXT_MODE>
void VJsonEachTableFunction<TEXT_MODE>::process_close() {
    _json_column = nullptr;
    _kv_pairs.first = nullptr;
    _kv_pairs.second = nullptr;
    _cur_size = 0;
}

template <bool TEXT_MODE>
void VJsonEachTableFunction<TEXT_MODE>::get_same_many_values(MutableColumnPtr& column, int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
        return;
    }

    ColumnStruct* ret;
    if (_is_nullable) {
        auto* nullable = assert_cast<ColumnNullable*>(column.get());
        ret = assert_cast<ColumnStruct*>(nullable->get_nested_column_ptr().get());
        assert_cast<ColumnUInt8*>(nullable->get_null_map_column_ptr().get())
                ->insert_many_defaults(length);
    } else {
        ret = assert_cast<ColumnStruct*>(column.get());
    }

    ret->get_column(0).insert_many_from(*_kv_pairs.first, _cur_offset, length);
    ret->get_column(1).insert_many_from(*_kv_pairs.second, _cur_offset, length);
}

template <bool TEXT_MODE>
int VJsonEachTableFunction<TEXT_MODE>::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));

    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        ColumnStruct* struct_col = nullptr;
        if (_is_nullable) {
            auto* nullable_col = assert_cast<ColumnNullable*>(column.get());
            struct_col = assert_cast<ColumnStruct*>(nullable_col->get_nested_column_ptr().get());
            assert_cast<ColumnUInt8*>(nullable_col->get_null_map_column_ptr().get())
                    ->insert_many_defaults(max_step);
        } else {
            struct_col = assert_cast<ColumnStruct*>(column.get());
        }

        struct_col->get_column(0).insert_range_from(*_kv_pairs.first, _cur_offset, max_step);
        struct_col->get_column(1).insert_range_from(*_kv_pairs.second, _cur_offset, max_step);
    }

    forward(max_step);
    return max_step;
}

// // Explicit template instantiations
template class VJsonEachTableFunction<false>; // json_each
template class VJsonEachTableFunction<true>;  // json_each_text

} // namespace doris
