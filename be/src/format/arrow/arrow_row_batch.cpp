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

#include "format/arrow/arrow_row_batch.h"

#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/extension/parquet_variant.h>
#include <arrow/extension/uuid.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_utils.h"
#include "runtime/descriptors.h"

namespace doris {

Status register_arrow_variant_extension() {
    // Remote Flight readers must restore the extension before decoding the result schema.
    static const auto status = [] {
        if (arrow::GetExtensionType("arrow.parquet.variant") != nullptr) {
            return arrow::Status::OK();
        }
        return arrow::RegisterExtensionType(
                std::static_pointer_cast<arrow::ExtensionType>(arrow::extension::variant(
                        arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                        arrow::field("value", arrow::binary(), false)}))));
    }();
    return status.ok() ? Status::OK() : Status::InternalError(status.ToString());
}

// Keep the exhaustive type mapping together so protocol-specific overrides remain visible.
// NOLINTNEXTLINE(readability-function-size)
Status DorisArrowSchemaConvertor::convert_to_arrow_type(
        const DataTypePtr& origin_type, std::shared_ptr<arrow::DataType>* result) const {
    auto type = get_serialized_type(origin_type);
    switch (type->get_primitive_type()) {
    case TYPE_NULL:
        *result = arrow::null();
        break;
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
    case TYPE_TIMEV2:
        *result = arrow::float64();
        break;
    case TYPE_IPV4:
        // ipv4 is uint32, but parquet not uint32, it's will be convert to int64
        // so use int32 directly
        *result = arrow::int32();
        break;
    case TYPE_IPV6:
        *result = arrow::utf8();
        break;
    case TYPE_UUID:
        *result = arrow::extension::uuid();
        break;
    case TYPE_LARGEINT:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_STRING:
    case TYPE_JSONB:
        *result = arrow::utf8();
        break;
    case TYPE_DATEV2:
        *result = std::make_shared<arrow::Date32Type>();
        break;
    case TYPE_TIMESTAMP_NS:
        // TIMESTAMP_NS is stored as signed epoch nanoseconds, but its SQL type has no timezone.
        *result = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO);
        break;
    case TYPE_TIMESTAMPTZ:
    case TYPE_DATETIMEV2: {
        arrow::TimeUnit::type time_unit;
        if (type->get_scale() > 3) {
            time_unit = arrow::TimeUnit::MICRO;
        } else if (type->get_scale() > 0) {
            time_unit = arrow::TimeUnit::MILLI;
        } else {
            time_unit = arrow::TimeUnit::SECOND;
        }
        *result = std::make_shared<arrow::TimestampType>(
                time_unit, timestamp_timezone(type->get_primitive_type()));
        break;
    }
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
        *result = std::make_shared<arrow::Decimal128Type>(type->get_precision(), type->get_scale());
        break;
    case TYPE_DECIMAL256:
        *result = std::make_shared<arrow::Decimal256Type>(type->get_precision(), type->get_scale());
        break;
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    case TYPE_ARRAY: {
        const auto* type_arr = assert_cast<const DataTypeArray*>(remove_nullable(type).get());
        std::shared_ptr<arrow::DataType> item_type;
        RETURN_IF_ERROR(convert_to_arrow_type(type_arr->get_nested_type(), &item_type));
        // Arrow stores metadata on fields, so implicit child fields lose the Doris logical type.
        *result = std::make_shared<arrow::ListType>(make_child_field(
                "item", item_type, true, type_arr->get_nested_type()->get_primitive_type()));
        break;
    }
    case TYPE_MAP: {
        const auto* type_map = assert_cast<const DataTypeMap*>(remove_nullable(type).get());
        std::shared_ptr<arrow::DataType> key_type;
        std::shared_ptr<arrow::DataType> val_type;
        RETURN_IF_ERROR(convert_to_arrow_type(type_map->get_key_type(), &key_type));
        RETURN_IF_ERROR(convert_to_arrow_type(type_map->get_value_type(), &val_type));
        auto key_field = make_child_field("key", key_type, false,
                                          type_map->get_key_type()->get_primitive_type());
        auto value_field = make_child_field("value", val_type, true,
                                            type_map->get_value_type()->get_primitive_type());
        *result = std::make_shared<arrow::MapType>(key_field, value_field);
        break;
    }
    case TYPE_STRUCT: {
        const auto* type_struct = assert_cast<const DataTypeStruct*>(remove_nullable(type).get());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (size_t i = 0; i < type_struct->get_elements().size(); i++) {
            std::shared_ptr<arrow::DataType> field_type;
            RETURN_IF_ERROR(convert_to_arrow_type(type_struct->get_element(i), &field_type));
            fields.push_back(make_child_field(type_struct->get_element_name(i), field_type,
                                              type_struct->get_element(i)->is_nullable(),
                                              type_struct->get_element(i)->get_primitive_type()));
        }
        *result = std::make_shared<arrow::StructType>(fields);
        break;
    }
    case TYPE_VARIANT:
        *result = arrow::utf8();
        break;
    case TYPE_QUANTILE_STATE:
    case TYPE_BITMAP:
    case TYPE_HLL: {
        *result = arrow::binary();
        break;
    }
    case TYPE_VARBINARY: {
        *result = arrow::binary();
        break;
    }
    default:
        return Status::InvalidArgument("Unknown primitive type({}) convert to Arrow type",
                                       type->get_name());
    }
    return Status::OK();
}

// Logical types sharing Arrow storage need the same marker at the root and every child field.
std::shared_ptr<arrow::Field> create_arrow_field_with_metadata(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
        bool is_nullable, PrimitiveType primitive_type) {
    const char* type_name;
    switch (primitive_type) {
    case TYPE_UUID:
        type_name = "UUID";
        break;
    case TYPE_IPV4:
        type_name = "IPV4";
        break;
    case TYPE_IPV6:
        type_name = "IPV6";
        break;
    case TYPE_LARGEINT:
        type_name = "LARGEINT";
        break;
    case TYPE_JSONB:
        type_name = "JSON";
        break;
    case TYPE_VARIANT:
        type_name = "VARIANT";
        break;
    default:
        return std::make_shared<arrow::Field>(field_name, arrow_type, is_nullable);
    }
    auto metadata = arrow::KeyValueMetadata::Make({"doris_type"}, {type_name});
    return std::make_shared<arrow::Field>(field_name, arrow_type, is_nullable, metadata);
}

Status DorisArrowSchemaConvertor::get_arrow_schema_from_block(
        const Block& block, std::shared_ptr<arrow::Schema>* result) const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& type_and_name : block) {
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_arrow_type(type_and_name.type, &arrow_type));
        auto field = make_field(type_and_name.name, arrow_type, type_and_name.type->is_nullable(),
                                type_and_name.type->get_primitive_type());
        fields.push_back(field);
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status DorisArrowSchemaConvertor::get_arrow_schema(std::shared_ptr<arrow::Schema>* result) const {
    return get_arrow_schema_from_block(_header, result);
}

std::string DorisArrowSchemaConvertor::timestamp_timezone(PrimitiveType) const {
    // Arrow clients expect a timezone name rather than the ISO-8601 UTC alias.
    return _timezone == "Z" ? "UTC" : _timezone;
}

Status ArrowFlightSchemaConvertor::convert_to_arrow_type(
        const DataTypePtr& type, std::shared_ptr<arrow::DataType>* result) const {
    // Flight always uses native Variant, including recursively converted children.
    if (type->get_primitive_type() == TYPE_VARIANT) {
        // Reject by type before reading rows, including empty results and nested legacy leaves.
        if (dynamic_cast<const DataTypeVariantV2*>(remove_nullable(type).get()) == nullptr) {
            return Status::NotSupported(
                    "Native Arrow Flight output only supports Variant V2, not legacy Variant; "
                    "cast the result to STRING for text output");
        }
        RETURN_IF_ERROR(register_arrow_variant_extension());
        *result = arrow::extension::variant(
                arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                arrow::field("value", arrow::binary(), false)}));
        return Status::OK();
    }
    return DorisArrowSchemaConvertor::convert_to_arrow_type(type, result);
}

std::string ArrowFlightSchemaConvertor::timestamp_timezone(PrimitiveType type) const {
    // DATETIMEV2 is wall-clock time; TIMESTAMPTZ must still describe an instant.
    return type == TYPE_DATETIMEV2 ? "" : DorisArrowSchemaConvertor::timestamp_timezone(type);
}

std::shared_ptr<arrow::Field> DorisArrowSchemaConvertor::make_field(
        const std::string& name, const std::shared_ptr<arrow::DataType>& type, bool nullable,
        PrimitiveType primitive) const {
    return create_arrow_field_with_metadata(name, type, nullable, primitive);
}

std::shared_ptr<arrow::Field> DorisArrowSchemaConvertor::make_child_field(
        const std::string& name, const std::shared_ptr<arrow::DataType>& type, bool nullable,
        PrimitiveType primitive) const {
    return create_arrow_field_with_metadata(name, type, nullable, primitive);
}

std::shared_ptr<arrow::Field> LegacyArrowFlightSchemaConvertor::make_field(
        const std::string& name, const std::shared_ptr<arrow::DataType>& type, bool nullable,
        PrimitiveType primitive) const {
    if (primitive == TYPE_JSONB || primitive == TYPE_VARIANT) {
        return arrow::field(name, type, nullable);
    }
    return DorisArrowSchemaConvertor::make_field(name, type, nullable, primitive);
}

std::shared_ptr<arrow::Field> LegacyArrowFlightSchemaConvertor::make_child_field(
        const std::string& name, const std::shared_ptr<arrow::DataType>& type, bool nullable,
        PrimitiveType) const {
    return arrow::field(name, type, nullable);
}

Status DorisArrowSchemaConvertor::get_arrow_schema_from_expr_ctxs(
        const VExprContextSPtrs& output_vexpr_ctxs, std::shared_ptr<arrow::Schema>* result) const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int i = 0; i < output_vexpr_ctxs.size(); i++) {
        std::shared_ptr<arrow::DataType> arrow_type;
        auto root_expr = output_vexpr_ctxs.at(i)->root();
        RETURN_IF_ERROR(convert_to_arrow_type(root_expr->data_type(), &arrow_type));
        auto field_name = root_expr->is_slot_ref() && !root_expr->expr_label().empty()
                                  ? root_expr->expr_label()
                                  : fmt::format("{}_{}", root_expr->data_type()->get_name(), i);
        auto field = make_field(field_name, arrow_type, root_expr->is_nullable(),
                                root_expr->data_type()->get_primitive_type());
        fields.push_back(std::move(field));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result) {
    // create sink memory buffer outputstream with the computed capacity
    int64_t capacity;
    arrow::Status a_st = arrow::ipc::GetRecordBatchSize(record_batch, &capacity);
    if (!a_st.ok()) {
        return Status::InternalError("GetRecordBatchSize failure, reason: {}", a_st.ToString());
    }
    auto sink_res = arrow::io::BufferOutputStream::Create(capacity, arrow::default_memory_pool());
    if (!sink_res.ok()) {
        return Status::InternalError("create BufferOutputStream failure, reason: {}",
                                     sink_res.status().ToString());
    }
    std::shared_ptr<arrow::io::BufferOutputStream> sink = sink_res.ValueOrDie();
    // create RecordBatch Writer
    auto res = arrow::ipc::MakeStreamWriter(sink.get(), record_batch.schema());
    if (!res.ok()) {
        return Status::InternalError("open RecordBatchStreamWriter failure, reason: {}",
                                     res.status().ToString());
    }
    // write RecordBatch to memory buffer outputstream
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer = res.ValueOrDie();
    a_st = record_batch_writer->WriteRecordBatch(record_batch);
    if (!a_st.ok()) {
        return Status::InternalError("write record batch failure, reason: {}", a_st.ToString());
    }
    a_st = record_batch_writer->Close();
    if (!a_st.ok()) {
        return Status::InternalError("Close failed, reason: {}", a_st.ToString());
    }
    auto finish_res = sink->Finish();
    if (!finish_res.ok()) {
        return Status::InternalError("allocate result buffer failure, reason: {}",
                                     finish_res.status().ToString());
    }
    *result = finish_res.ValueOrDie()->ToString();
    // close the sink
    a_st = sink->Close();
    if (!a_st.ok()) {
        return Status::InternalError("Close failed, reason: {}", a_st.ToString());
    }
    return Status::OK();
}

Status serialize_arrow_schema(std::shared_ptr<arrow::Schema>* schema, std::string* result) {
    // Schema RPC readers only consume the IPC schema. Building an empty batch would require
    // nested extension builders, which Arrow does not provide for ARRAY/MAP/STRUCT<VARIANT>.
    std::shared_ptr<arrow::io::BufferOutputStream> sink;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(sink, arrow::io::BufferOutputStream::Create());
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(writer, arrow::ipc::MakeStreamWriter(sink.get(), *schema));
    RETURN_DORIS_STATUS_IF_ERROR(writer->Close());
    std::shared_ptr<arrow::Buffer> buffer;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(buffer, sink->Finish());
    *result = buffer->ToString();
    return Status::OK();
}

} // namespace doris
