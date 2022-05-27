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

#include "util/arrow/row_block.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/visit_array_inline.h>
#include <arrow/visit_type_inline.h>

#include "gutil/strings/substitute.h"
#include "olap/column_block.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/row_block2.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "util/arrow/utils.h"

namespace doris {

using strings::Substitute;

Status convert_to_arrow_type(FieldType type, std::shared_ptr<arrow::DataType>* result) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case OLAP_FIELD_TYPE_INT:
        *result = arrow::int32();
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case OLAP_FIELD_TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case OLAP_FIELD_TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown FieldType($0)", type));
    }
    return Status::OK();
}

Status convert_to_arrow_field(uint32_t cid, const Field* field,
                              std::shared_ptr<arrow::Field>* result) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(field->type(), &type));
    *result = arrow::field(strings::Substitute("Col$0", cid), type, field->is_nullable());
    return Status::OK();
}

Status convert_to_arrow_schema(const Schema& schema, std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    size_t num_fields = schema.num_column_ids();
    fields.resize(num_fields);
    for (int i = 0; i < num_fields; ++i) {
        auto cid = schema.column_ids()[i];
        RETURN_IF_ERROR(convert_to_arrow_field(cid, schema.column(cid), &fields[i]));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status convert_to_type_name(const arrow::DataType& type, std::string* name) {
    switch (type.id()) {
    case arrow::Type::INT8:
        *name = "TINYINT";
        break;
    case arrow::Type::INT16:
        *name = "SMALLINT";
        break;
    case arrow::Type::INT32:
        *name = "INT";
        break;
    case arrow::Type::INT64:
        *name = "BIGINT";
        break;
    case arrow::Type::FLOAT:
        *name = "FLOAT";
        break;
    case arrow::Type::DOUBLE:
        *name = "DOUBLE";
        break;
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown arrow type id($0)", type.id()));
    }
    return Status::OK();
}

Status convert_to_tablet_column(const arrow::Field& field, int32_t cid, TabletColumn* output) {
    ColumnPB column_pb;
    std::string type_name;
    RETURN_IF_ERROR(convert_to_type_name(*field.type(), &type_name));

    column_pb.set_unique_id(cid);
    column_pb.set_name(field.name());
    column_pb.set_type(type_name);
    column_pb.set_is_key(true);
    column_pb.set_is_nullable(field.nullable());

    output->init_from_pb(column_pb);
    return Status::OK();
}

Status convert_to_doris_schema(const arrow::Schema& schema, std::shared_ptr<Schema>* result) {
    auto num_fields = schema.num_fields();
    std::vector<TabletColumn> columns(num_fields);
    std::vector<ColumnId> col_ids(num_fields);
    for (int i = 0; i < num_fields; ++i) {
        RETURN_IF_ERROR(convert_to_tablet_column(*schema.field(i), i, &columns[i]));
        col_ids[i] = i;
    }
    result->reset(new Schema(columns, col_ids));
    return Status::OK();
}

// Convert data in RowBlockV2 to an Arrow RecordBatch
// We should keep this function to keep compatible with arrow's type visitor
// Now we inherit TypeVisitor to use default Visit implementation
class FromRowBlockConverter : public arrow::TypeVisitor {
public:
    FromRowBlockConverter(const RowBlockV2& block, const std::shared_ptr<arrow::Schema>& schema,
                          arrow::MemoryPool* pool)
            : _block(block), _schema(schema), _pool(pool) {}

    ~FromRowBlockConverter() override {}

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

    Status convert(std::shared_ptr<arrow::RecordBatch>* out);

private:
    template <typename T>
    typename std::enable_if<std::is_base_of<arrow::PrimitiveCType, T>::value, arrow::Status>::type
    _visit(const T& type) {
        arrow::NumericBuilder<T> builder(_pool);
        size_t num_rows = _block.num_rows();
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));

        auto column_block = _block.column_block(_cur_field_idx);
        for (size_t i = 0; i < num_rows; ++i) {
            if (column_block.is_null(i)) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            } else {
                auto cell_ptr = column_block.cell_ptr(i);
                ARROW_RETURN_NOT_OK(builder.Append(*(typename T::c_type*)cell_ptr));
            }
        }
        return builder.Finish(&_arrays[_cur_field_idx]);
    }

private:
    const RowBlockV2& _block;
    std::shared_ptr<arrow::Schema> _schema;
    arrow::MemoryPool* _pool;

    size_t _cur_field_idx = 0;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

Status FromRowBlockConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    size_t num_fields = _schema->num_fields();
    if (num_fields != _block.schema()->num_column_ids()) {
        return Status::InvalidArgument("Schema not match");
    }

    _arrays.resize(num_fields);
    for (int idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        auto arrow_st = arrow::VisitTypeInline(*_schema->field(idx)->type(), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
        }
    }
    *out = arrow::RecordBatch::Make(_schema, _block.num_rows(), std::move(_arrays));
    return Status::OK();
}

Status convert_to_arrow_batch(const RowBlockV2& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result) {
    FromRowBlockConverter converter(block, schema, pool);
    return converter.convert(result);
}

// Convert Arrow RecordBatch to Doris RowBlockV2
class ToRowBlockConverter : public arrow::ArrayVisitor {
public:
    ToRowBlockConverter(const arrow::RecordBatch& batch, const Schema& schema)
            : _batch(batch), _schema(schema) {}

    ~ToRowBlockConverter() override {}

    using arrow::ArrayVisitor::Visit;

#define PRIMITIVE_VISIT(TYPE) \
    arrow::Status Visit(const arrow::TYPE& array) override { return _visit(array); }

    PRIMITIVE_VISIT(Int8Array);
    PRIMITIVE_VISIT(Int16Array);
    PRIMITIVE_VISIT(Int32Array);
    PRIMITIVE_VISIT(Int64Array);
    PRIMITIVE_VISIT(FloatArray);
    PRIMITIVE_VISIT(DoubleArray);

#undef PRIMITIVE_VISIT

    Status convert(std::shared_ptr<RowBlockV2>* result);

private:
    template <typename T>
    typename std::enable_if<std::is_base_of<arrow::PrimitiveCType, typename T::TypeClass>::value,
                            arrow::Status>::type
    _visit(const T& array) {
        auto raw_values = array.raw_values();
        auto column_block = _output->column_block(_cur_field_idx);
        for (size_t idx = 0; idx < array.length(); ++idx) {
            if (array.IsValid(idx)) {
                auto cell_ptr = column_block.mutable_cell_ptr(idx);
                column_block.set_is_null(idx, false);
                *(typename T::TypeClass::c_type*)cell_ptr = raw_values[idx];
            } else {
                column_block.set_is_null(idx, true);
            }
        }
        return arrow::Status::OK();
    }

private:
    const arrow::RecordBatch& _batch;
    const Schema& _schema;

    size_t _cur_field_idx;

    std::shared_ptr<RowBlockV2> _output;
};

Status ToRowBlockConverter::convert(std::shared_ptr<RowBlockV2>* result) {
    size_t num_fields = _schema.num_column_ids();
    if (_batch.schema()->num_fields() != num_fields) {
        return Status::InvalidArgument("Schema not match");
    }

    auto num_rows = _batch.num_rows();
    _output.reset(new RowBlockV2(_schema, num_rows));
    for (int idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        auto arrow_st = arrow::VisitArrayInline(*_batch.column(idx), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
        }
    }
    _output->set_num_rows(num_rows);
    *result = std::move(_output);
    return Status::OK();
}

Status convert_to_row_block(const arrow::RecordBatch& batch, const Schema& schema,
                            std::shared_ptr<RowBlockV2>* result) {
    ToRowBlockConverter converter(batch, schema);
    return converter.convert(result);
}

} // namespace doris
