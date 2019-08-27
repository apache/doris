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

#include "util/arrow/row_batch.h"

#include <arrow/visitor.h>
#include <arrow/visitor_inline.h>
#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/array/builder_primitive.h>

#include "exprs/slot_ref.h"
#include "gutil/strings/substitute.h"
#include "runtime/row_batch.h"
#include "runtime/descriptors.h"
#include "runtime/descriptor_helper.h"
#include "util/arrow/utils.h"

namespace doris {

using strings::Substitute;

Status convert_to_arrow_type(const TypeDescriptor& type,
                             std::shared_ptr<arrow::DataType>* result) {
    switch (type.type) {
    case TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case TYPE_INT:
        *result = arrow::int32();
        break;
    case TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    default:
        return Status::InvalidArgument(Substitute("Unknown primitive type($0)", type.type));
    }
    return Status::OK();
}

Status convert_to_arrow_field(SlotDescriptor* desc,
                              std::shared_ptr<arrow::Field>* field) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(desc->type(), &type));
    *field = arrow::field(desc->col_name(), type, desc->is_nullable());
    return Status::OK();
}

Status convert_to_arrow_schema(
        const RowDescriptor& row_desc,
        std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto tuple_desc : row_desc.tuple_descriptors()) {
        for (auto desc : tuple_desc->slots()) {
            std::shared_ptr<arrow::Field> field;
            RETURN_IF_ERROR(convert_to_arrow_field(desc, &field));
            fields.push_back(field);
        }
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status convert_to_doris_type(const arrow::DataType& type,
                             TSlotDescriptorBuilder* builder) {
    switch (type.id()) {
    case arrow::Type::INT8:
        builder->type(TYPE_TINYINT);
        break;
    case arrow::Type::INT16:
        builder->type(TYPE_SMALLINT);
        break;
    case arrow::Type::INT32:
        builder->type(TYPE_INT);
        break;
    case arrow::Type::INT64:
        builder->type(TYPE_BIGINT);
        break;
    case arrow::Type::FLOAT:
        builder->type(TYPE_FLOAT);
        break;
    case arrow::Type::DOUBLE:
        builder->type(TYPE_DOUBLE);
        break;
    default:
        return Status::InvalidArgument(Substitute("Unknown arrow type id($0)", type.id()));
    }
    return Status::OK();
}

Status convert_to_slot_desc(const arrow::Field& field,
                            int column_pos,
                            TSlotDescriptorBuilder* builder) {
    RETURN_IF_ERROR(convert_to_doris_type(*field.type(), builder));
    builder->column_name(field.name()).nullable(field.nullable()).column_pos(column_pos);
    return Status::OK();
}

Status convert_to_row_desc(
        ObjectPool* pool,
        const arrow::Schema& schema,
        RowDescriptor** row_desc) {
    TDescriptorTableBuilder builder;
    TTupleDescriptorBuilder tuple_builder;
    for (int i = 0; i < schema.num_fields(); ++i) {
        auto field = schema.field(i);
        TSlotDescriptorBuilder slot_builder;
        RETURN_IF_ERROR(convert_to_slot_desc(*field, i, &slot_builder));
        tuple_builder.add_slot(slot_builder.build());
    }
    tuple_builder.build(&builder);
    DescriptorTbl* tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(pool, builder.desc_tbl(), &tbl));
    auto tuple_desc = tbl->get_tuple_descriptor(0);
    *row_desc = pool->add(new RowDescriptor(tuple_desc, false));
    return Status::OK();
}

// Convert RowBatch to an Arrow::Array
// We should keep this function to keep compatible with arrow's type visitor
// Now we inherit TypeVisitor to use default Visit implemention
class FromRowBatchConverter : public arrow::TypeVisitor {
public:
    FromRowBatchConverter(const RowBatch& batch,
                          const std::shared_ptr<arrow::Schema>& schema,
                          arrow::MemoryPool* pool)
        : _batch(batch),
        _schema(schema),
        _pool(pool) { }

    ~FromRowBatchConverter() override { }

    // Use base class function
    using arrow::TypeVisitor::Visit;

#define PRIMITIVE_VISIT(TYPE) \
    arrow::Status Visit(const arrow::TYPE& type) override { \
        return _visit(type); \
    }

    PRIMITIVE_VISIT(Int8Type);
    PRIMITIVE_VISIT(Int16Type);
    PRIMITIVE_VISIT(Int32Type);
    PRIMITIVE_VISIT(Int64Type);
    PRIMITIVE_VISIT(FloatType);
    PRIMITIVE_VISIT(DoubleType);

#undef PRIMITIVE_VISIT

    Status convert(std::shared_ptr<arrow::RecordBatch>* out);

private:

    template<typename T>
    typename std::enable_if<std::is_base_of<arrow::PrimitiveCType, T>::value,
             arrow::Status>::type
    _visit(const T& type) {
        arrow::NumericBuilder<T> builder(_pool);

        size_t num_rows = _batch.num_rows();
        builder.Reserve(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            auto cell_ptr = _cur_slot_ref->get_slot(_batch.get_row(i));
            if (cell_ptr == nullptr) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            } else {
                ARROW_RETURN_NOT_OK(builder.Append(*(typename T::c_type*)cell_ptr));
            }
        }

        return builder.Finish(&_arrays[_cur_field_idx]);
    }

private:
    const RowBatch& _batch;
    const std::shared_ptr<arrow::Schema>& _schema;
    arrow::MemoryPool* _pool;

    size_t _cur_field_idx;
    std::unique_ptr<SlotRef> _cur_slot_ref;

    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

Status FromRowBatchConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    std::vector<SlotDescriptor*> slot_descs;
    for (auto tuple_desc : _batch.row_desc().tuple_descriptors()) {
        for (auto desc : tuple_desc->slots()) {
            slot_descs.push_back(desc);
        }
    }
    size_t num_fields = _schema->num_fields();
    if (slot_descs.size() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }

    _arrays.resize(num_fields);

    for (size_t idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        _cur_slot_ref.reset(new SlotRef(slot_descs[idx]));
        RETURN_IF_ERROR(_cur_slot_ref->prepare(slot_descs[idx], _batch.row_desc()));
        auto arrow_st = arrow::VisitTypeInline(*_schema->field(idx)->type(), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
        }
    }
    *out = arrow::RecordBatch::Make(_schema, _batch.num_rows(), std::move(_arrays));
    return Status::OK();
}

Status convert_to_arrow_batch(
        const RowBatch& batch,
        const std::shared_ptr<arrow::Schema>& schema,
        arrow::MemoryPool* pool,
        std::shared_ptr<arrow::RecordBatch>* result) {
    FromRowBatchConverter converter(batch, schema, pool);
    return converter.convert(result);
}

// Convert Arrow Array to RowBatch
class ToRowBatchConverter : public arrow::ArrayVisitor {
public:
    using arrow::ArrayVisitor::Visit;

    ToRowBatchConverter(const arrow::RecordBatch& batch,
                        const RowDescriptor& row_desc,
                        MemTracker* tracker)
        : _batch(batch), _row_desc(row_desc), _tracker(tracker) { }

#define PRIMITIVE_VISIT(TYPE) \
    arrow::Status Visit(const arrow::TYPE& array) override { \
        return _visit(array); \
    }

    PRIMITIVE_VISIT(Int8Array);
    PRIMITIVE_VISIT(Int16Array);
    PRIMITIVE_VISIT(Int32Array);
    PRIMITIVE_VISIT(Int64Array);
    PRIMITIVE_VISIT(FloatArray);
    PRIMITIVE_VISIT(DoubleArray);

#undef PRIMITIVE_VISIT

    // Convert to a RowBatch
    Status convert(std::shared_ptr<RowBatch>* result);

private:

    template<typename T>
    typename std::enable_if<std::is_base_of<arrow::PrimitiveCType, typename T::TypeClass>::value,
             arrow::Status>::type
    _visit(const T& array) {
        auto raw_values = array.raw_values();
        for (size_t i = 0; i < array.length(); ++i) {
            auto row = _output->get_row(i);
            auto tuple = _cur_slot_ref->get_tuple(row);
            if (array.IsValid(i)) {
                tuple->set_not_null(_cur_slot_ref->null_indicator_offset());
                auto slot = _cur_slot_ref->get_slot(row);
                *(typename T::TypeClass::c_type*)slot = raw_values[i];
            } else {
                tuple->set_null(_cur_slot_ref->null_indicator_offset());
            }
        }
        return arrow::Status::OK();
    }

private:
    const arrow::RecordBatch& _batch;
    const RowDescriptor& _row_desc;
    MemTracker* _tracker;

    std::unique_ptr<SlotRef> _cur_slot_ref;
    std::shared_ptr<RowBatch> _output;
};

Status ToRowBatchConverter:: convert(std::shared_ptr<RowBatch>* result) {
    std::vector<SlotDescriptor*> slot_descs;
    for (auto tuple_desc : _row_desc.tuple_descriptors()) {
        for (auto desc : tuple_desc->slots()) {
            slot_descs.push_back(desc);
        }
    }
    size_t num_fields = slot_descs.size();
    if (num_fields != _batch.schema()->num_fields()) {
        return Status::InvalidArgument("Schema not match");
    }
    // TODO(zc): check if field type match

    size_t num_rows = _batch.num_rows();
    _output.reset(new RowBatch(_row_desc, num_rows, _tracker));
    _output->commit_rows(num_rows);
    auto pool = _output->tuple_data_pool();
    for (size_t row_id = 0; row_id < num_rows; ++row_id) {
        auto row = _output->get_row(row_id);
        for (int tuple_id = 0; tuple_id < _row_desc.tuple_descriptors().size(); ++tuple_id) {
            auto tuple_desc = _row_desc.tuple_descriptors()[tuple_id];
            auto tuple = pool->allocate(tuple_desc->byte_size());
            row->set_tuple(tuple_id, (Tuple*)tuple);
        }
    }
    for (size_t idx = 0; idx < num_fields; ++idx) {
        _cur_slot_ref.reset(new SlotRef(slot_descs[idx]));
        RETURN_IF_ERROR(_cur_slot_ref->prepare(slot_descs[idx], _row_desc));
        auto arrow_st = arrow::VisitArrayInline(*_batch.column(idx), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
        }
    }

    *result = std::move(_output);

    return Status::OK();
}

Status convert_to_row_batch(const arrow::RecordBatch& batch,
                            const RowDescriptor& row_desc,
                            MemTracker* tracker,
                            std::shared_ptr<RowBatch>* result) {
    ToRowBatchConverter converter(batch, row_desc, tracker);
    return converter.convert(result);
}

}

