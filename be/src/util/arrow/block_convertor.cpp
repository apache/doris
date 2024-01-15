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

#include "util/arrow/block_convertor.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <ctime>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gutil/integral_types.h"
#include "runtime/large_int_value.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"
#include "util/jsonb_utils.h"
#include "util/types.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace arrow {
class Array;
} // namespace arrow

namespace doris {

// Convert Block to an Arrow::Array
// We should keep this function to keep compatible with arrow's type visitor
// Now we inherit TypeVisitor to use default Visit implementation
class FromBlockConverter : public arrow::TypeVisitor {
public:
    FromBlockConverter(const vectorized::Block& block, const std::shared_ptr<arrow::Schema>& schema,
                       arrow::MemoryPool* pool)
            : _block(block), _schema(schema), _pool(pool), _cur_field_idx(-1) {
        // obtain local time zone
        time_t ts = 0;
        struct tm t;
        char buf[16];
        localtime_r(&ts, &t);
        strftime(buf, sizeof(buf), "%Z", &t);
        _time_zone = buf;
    }

    ~FromBlockConverter() override = default;

    // Use base class function
    using arrow::TypeVisitor::Visit;

#define PRIMITIVE_VISIT(TYPE) \
    arrow::Status Visit(const arrow::TYPE& type) override { return _visit(type); }

    PRIMITIVE_VISIT(Int8Type)
    PRIMITIVE_VISIT(Int16Type)
    PRIMITIVE_VISIT(Int32Type)
    PRIMITIVE_VISIT(Int64Type)
    PRIMITIVE_VISIT(FloatType)
    PRIMITIVE_VISIT(DoubleType)

#undef PRIMITIVE_VISIT

    // process string-transformable field
    arrow::Status Visit(const arrow::StringType& type) override {
        auto& builder = assert_cast<arrow::StringBuilder&>(*_cur_builder);
        size_t start = _cur_start;
        size_t num_rows = _cur_rows;
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        for (size_t i = start; i < start + num_rows; ++i) {
            bool is_null = _cur_col->is_null_at(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto& data_ref = _cur_col->get_data_at(i);
            vectorized::TypeIndex type_idx = vectorized::remove_nullable(_cur_type)->get_type_id();
            switch (type_idx) {
            case vectorized::TypeIndex::String:
            case vectorized::TypeIndex::FixedString:
            case vectorized::TypeIndex::HLL: {
                if (data_ref.size == 0) {
                    // 0x01 is a magic num, not useful actually, just for present ""
                    //char* tmp_val = reinterpret_cast<char*>(0x01);
                    ARROW_RETURN_NOT_OK(builder.Append(""));
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(data_ref.data, data_ref.size));
                }
                break;
            }
            case vectorized::TypeIndex::Date:
            case vectorized::TypeIndex::DateTime: {
                char buf[64];
                const VecDateTimeValue* time_val = (const VecDateTimeValue*)(data_ref.data);
                int len = time_val->to_buffer(buf);
                ARROW_RETURN_NOT_OK(builder.Append(buf, len));
                break;
            }
            case vectorized::TypeIndex::DateV2: {
                char buf[64];
                const DateV2Value<DateV2ValueType>* time_val =
                        (const DateV2Value<DateV2ValueType>*)(data_ref.data);
                int len = time_val->to_buffer(buf);
                ARROW_RETURN_NOT_OK(builder.Append(buf, len));
                break;
            }
            case vectorized::TypeIndex::DateTimeV2: {
                char buf[64];
                const DateV2Value<DateTimeV2ValueType>* time_val =
                        (const DateV2Value<DateTimeV2ValueType>*)(data_ref.data);
                int len = time_val->to_buffer(buf);
                ARROW_RETURN_NOT_OK(builder.Append(buf, len));
                break;
            }
            case vectorized::TypeIndex::Int128: {
                auto string_temp = LargeIntValue::to_string(
                        reinterpret_cast<const PackedInt128*>(data_ref.data)->value);
                ARROW_RETURN_NOT_OK(builder.Append(string_temp.data(), string_temp.size()));
                break;
            }
            case vectorized::TypeIndex::JSONB: {
                std::string string_temp =
                        JsonbToJson::jsonb_to_json_string(data_ref.data, data_ref.size);
                ARROW_RETURN_NOT_OK(builder.Append(string_temp.data(), string_temp.size()));
                break;
            }
            default: {
                LOG(WARNING) << "can't convert this type = " << vectorized::getTypeName(type_idx)
                             << " to arrow type";
                return arrow::Status::TypeError("unsupported column type");
            }
            }
        }
        return arrow::Status::OK();
    }

    // process doris Decimal
    arrow::Status Visit(const arrow::Decimal128Type& type) override {
        auto& builder = assert_cast<arrow::Decimal128Builder&>(*_cur_builder);
        size_t start = _cur_start;
        size_t num_rows = _cur_rows;
        if (auto* decimalv2_column = vectorized::check_and_get_column<
                    vectorized::ColumnDecimal<vectorized::Decimal128V2>>(
                    *vectorized::remove_nullable(_cur_col))) {
            std::shared_ptr<arrow::DataType> s_decimal_ptr =
                    std::make_shared<arrow::Decimal128Type>(27, 9);
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (size_t i = start; i < start + num_rows; ++i) {
                bool is_null = _cur_col->is_null_at(i);
                if (is_null) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                const auto& data_ref = decimalv2_column->get_data_at(i);
                const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
                int64_t high = (p_value->value) >> 64;
                uint64 low = p_value->value;
                arrow::Decimal128 value(high, low);
                ARROW_RETURN_NOT_OK(builder.Append(value));
            }
            return arrow::Status::OK();
        } else if (auto* decimal128_column = vectorized::check_and_get_column<
                           vectorized::ColumnDecimal<vectorized::Decimal128V3>>(
                           *vectorized::remove_nullable(_cur_col))) {
            std::shared_ptr<arrow::DataType> s_decimal_ptr =
                    std::make_shared<arrow::Decimal128Type>(38, decimal128_column->get_scale());
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (size_t i = start; i < start + num_rows; ++i) {
                bool is_null = _cur_col->is_null_at(i);
                if (is_null) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                const auto& data_ref = decimal128_column->get_data_at(i);
                const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
                int64_t high = (p_value->value) >> 64;
                uint64 low = p_value->value;
                arrow::Decimal128 value(high, low);
                ARROW_RETURN_NOT_OK(builder.Append(value));
            }
            return arrow::Status::OK();
        } else if (auto* decimal32_column = vectorized::check_and_get_column<
                           vectorized::ColumnDecimal<vectorized::Decimal32>>(
                           *vectorized::remove_nullable(_cur_col))) {
            std::shared_ptr<arrow::DataType> s_decimal_ptr =
                    std::make_shared<arrow::Decimal128Type>(8, decimal32_column->get_scale());
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (size_t i = start; i < start + num_rows; ++i) {
                bool is_null = _cur_col->is_null_at(i);
                if (is_null) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                const auto& data_ref = decimal32_column->get_data_at(i);
                const int32_t* p_value = reinterpret_cast<const int32_t*>(data_ref.data);
                int64_t high = *p_value > 0 ? 0 : 1UL << 63;
                arrow::Decimal128 value(high, *p_value > 0 ? *p_value : -*p_value);
                ARROW_RETURN_NOT_OK(builder.Append(value));
            }
            return arrow::Status::OK();
        } else if (auto* decimal64_column = vectorized::check_and_get_column<
                           vectorized::ColumnDecimal<vectorized::Decimal64>>(
                           *vectorized::remove_nullable(_cur_col))) {
            std::shared_ptr<arrow::DataType> s_decimal_ptr =
                    std::make_shared<arrow::Decimal128Type>(18, decimal64_column->get_scale());
            ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
            for (size_t i = start; i < start + num_rows; ++i) {
                bool is_null = _cur_col->is_null_at(i);
                if (is_null) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                const auto& data_ref = decimal64_column->get_data_at(i);
                const int64_t* p_value = reinterpret_cast<const int64_t*>(data_ref.data);
                int64_t high = *p_value > 0 ? 0 : 1UL << 63;
                arrow::Decimal128 value(high, *p_value > 0 ? *p_value : -*p_value);
                ARROW_RETURN_NOT_OK(builder.Append(value));
            }
            return arrow::Status::OK();
        } else {
            return arrow::Status::TypeError("Unsupported column:" + _cur_col->get_name());
        }
    }
    // process boolean
    arrow::Status Visit(const arrow::BooleanType& type) override {
        auto& builder = assert_cast<arrow::BooleanBuilder&>(*_cur_builder);
        size_t start = _cur_start;
        size_t num_rows = _cur_rows;
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        for (size_t i = start; i < start + num_rows; ++i) {
            bool is_null = _cur_col->is_null_at(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto& data_ref = _cur_col->get_data_at(i);
            ARROW_RETURN_NOT_OK(builder.Append(*(const bool*)data_ref.data));
        }
        return arrow::Status::OK();
    }

    // process array type
    arrow::Status Visit(const arrow::ListType& type) override {
        auto& builder = assert_cast<arrow::ListBuilder&>(*_cur_builder);
        auto orignal_col = _cur_col;
        size_t start = _cur_start;
        size_t num_rows = _cur_rows;

        const vectorized::ColumnArray* array_column = nullptr;
        if (orignal_col->is_nullable()) {
            auto nullable_column =
                    assert_cast<const vectorized::ColumnNullable*>(orignal_col.get());
            array_column = assert_cast<const vectorized::ColumnArray*>(
                    &nullable_column->get_nested_column());
        } else {
            array_column = assert_cast<const vectorized::ColumnArray*>(orignal_col.get());
        }
        const auto& offsets = array_column->get_offsets();
        vectorized::ColumnPtr nested_column = array_column->get_data_ptr();

        // set current col/type/builder to nested
        _cur_col = nested_column;
        if (_cur_type->is_nullable()) {
            auto nullable_type = assert_cast<const vectorized::DataTypeNullable*>(_cur_type.get());
            _cur_type = assert_cast<const vectorized::DataTypeArray*>(
                                nullable_type->get_nested_type().get())
                                ->get_nested_type();
        } else {
            _cur_type = assert_cast<const vectorized::DataTypeArray*>(_cur_type.get())
                                ->get_nested_type();
        }
        _cur_builder = builder.value_builder();

        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        for (size_t i = start; i < start + num_rows; ++i) {
            bool is_null = orignal_col->is_null_at(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            // append array elements in row i
            ARROW_RETURN_NOT_OK(builder.Append());
            _cur_start = offsets[i - 1];
            _cur_rows = offsets[i] - offsets[i - 1];
            ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*type.value_type(), this));
        }

        return arrow::Status::OK();
    }

    Status convert(std::shared_ptr<arrow::RecordBatch>* out);

private:
    template <typename T>
    arrow::Status _visit(const T& type) {
        auto& builder = assert_cast<arrow::NumericBuilder<T>&>(*_cur_builder);
        size_t start = _cur_start;
        size_t num_rows = _cur_rows;
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        if (_cur_col->is_nullable()) {
            for (size_t i = start; i < start + num_rows; ++i) {
                bool is_null = _cur_col->is_null_at(i);
                if (is_null) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                const auto& data_ref = _cur_col->get_data_at(i);
                ARROW_RETURN_NOT_OK(builder.Append(*(const typename T::c_type*)data_ref.data));
            }
        } else {
            ARROW_RETURN_NOT_OK(builder.AppendValues(
                    (const typename T::c_type*)_cur_col->get_data_at(start).data, num_rows));
        }
        return arrow::Status::OK();
    }

    const vectorized::Block& _block;
    const std::shared_ptr<arrow::Schema>& _schema;
    arrow::MemoryPool* _pool;

    size_t _cur_field_idx;
    size_t _cur_start;
    size_t _cur_rows;
    vectorized::ColumnPtr _cur_col;
    vectorized::DataTypePtr _cur_type;
    arrow::ArrayBuilder* _cur_builder = nullptr;

    std::string _time_zone;

    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

Status FromBlockConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    size_t num_fields = _schema->num_fields();
    if (_block.columns() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }

    _arrays.resize(num_fields);

    for (size_t idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        _cur_start = 0;
        _cur_rows = _block.rows();
        _cur_col = _block.get_by_position(idx).column;
        _cur_type = _block.get_by_position(idx).type;
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto arrow_st = arrow::MakeBuilder(_pool, _schema->field(idx)->type(), &builder);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        _cur_builder = builder.get();
        auto column = _cur_col->convert_to_full_column_if_const();
        try {
            _cur_type->get_serde()->write_column_to_arrow(*column, nullptr, _cur_builder,
                                                          _cur_start, _cur_start + _cur_rows);
        } catch (std::exception& e) {
            return Status::InternalError("Fail to convert block data to arrow data, error: {}",
                                         e.what());
        }
        arrow_st = _cur_builder->Finish(&_arrays[_cur_field_idx]);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
    }
    *out = arrow::RecordBatch::Make(_schema, _block.rows(), std::move(_arrays));
    return Status::OK();
}

Status convert_to_arrow_batch(const vectorized::Block& block,
                              const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result) {
    FromBlockConverter converter(block, schema, pool);
    return converter.convert(result);
}

} // namespace doris
