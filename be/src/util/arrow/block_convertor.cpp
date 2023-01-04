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

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/visit_array_inline.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>

#include <cstdlib>
#include <ctime>
#include <memory>

#include "exprs/slot_ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/large_int_value.h"
#include "util/arrow/utils.h"
#include "util/types.h"

namespace doris {

// Convert RowBatch to an Arrow::Array
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

    PRIMITIVE_VISIT(Int8Type);
    PRIMITIVE_VISIT(Int16Type);
    PRIMITIVE_VISIT(Int32Type);
    PRIMITIVE_VISIT(Int64Type);
    PRIMITIVE_VISIT(FloatType);
    PRIMITIVE_VISIT(DoubleType);

#undef PRIMITIVE_VISIT

    // process string-transformable field
    arrow::Status Visit(const arrow::StringType& type) override {
        arrow::StringBuilder builder(_pool);
        size_t num_rows = _block.rows();
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        for (size_t i = 0; i < num_rows; ++i) {
            bool is_null = _cur_col->is_null_at(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto& data_ref = _cur_col->get_data_at(i);
            vectorized::TypeIndex type_idx = _cur_type->get_type_id();
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
                const DateTimeValue* time_val = (const DateTimeValue*)(data_ref.data);
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
            default: {
                LOG(WARNING) << "can't convert this type = " << vectorized::getTypeName(type_idx)
                             << "to arrow type";
                return arrow::Status::TypeError("unsupported column type");
            }
            }
        }
        return builder.Finish(&_arrays[_cur_field_idx]);
    }

    // process doris DecimalV2
    arrow::Status Visit(const arrow::Decimal128Type& type) override {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(27, 9);
        arrow::Decimal128Builder builder(s_decimal_ptr, _pool);
        size_t num_rows = _block.rows();
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        for (size_t i = 0; i < num_rows; ++i) {
            bool is_null = _cur_col->is_null_at(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto& data_ref = _cur_col->get_data_at(i);
            const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
            int64_t high = (p_value->value) >> 64;
            uint64 low = p_value->value;
            arrow::Decimal128 value(high, low);
            ARROW_RETURN_NOT_OK(builder.Append(value));
        }
        return builder.Finish(&_arrays[_cur_field_idx]);
    }
    // process boolean
    arrow::Status Visit(const arrow::BooleanType& type) override {
        arrow::BooleanBuilder builder(_pool);
        size_t num_rows = _block.rows();
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        for (size_t i = 0; i < num_rows; ++i) {
            bool is_null = _cur_col->is_null_at(i);
            if (is_null) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            const auto& data_ref = _cur_col->get_data_at(i);
            ARROW_RETURN_NOT_OK(builder.Append(*(const bool*)data_ref.data));
        }
        return builder.Finish(&_arrays[_cur_field_idx]);
    }

    Status convert(std::shared_ptr<arrow::RecordBatch>* out);

private:
    template <typename T>
    arrow::Status _visit(const T& type) {
        arrow::NumericBuilder<T> builder(_pool);

        size_t num_rows = _block.rows();
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        if (_cur_col->is_nullable()) {
            for (size_t i = 0; i < num_rows; ++i) {
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
                    (const typename T::c_type*)_cur_col->get_data_at(0).data, num_rows));
        }
        return builder.Finish(&_arrays[_cur_field_idx]);
    }

    const vectorized::Block& _block;
    const std::shared_ptr<arrow::Schema>& _schema;
    arrow::MemoryPool* _pool;

    size_t _cur_field_idx;
    vectorized::ColumnPtr _cur_col;
    vectorized::DataTypePtr _cur_type;

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
        _cur_col = _block.get_by_position(idx).column;
        _cur_type = _block.get_by_position(idx).type;
        auto arrow_st = arrow::VisitTypeInline(*_schema->field(idx)->type(), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
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
