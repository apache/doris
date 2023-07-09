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

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "gutil/strings/substitute.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace doris {

using strings::Substitute;

Status convert_to_arrow_type(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result) {
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
    case TYPE_TIME:
        *result = arrow::float64();
        break;
    case TYPE_LARGEINT:
        *result = arrow::fixed_size_binary(sizeof(int128_t));
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DATEV2:
    case TYPE_DATETIMEV2:
    case TYPE_STRING:
    case TYPE_JSONB:
        *result = arrow::utf8();
        break;
    case TYPE_DECIMALV2:
        *result = std::make_shared<arrow::Decimal128Type>(27, 9);
        break;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
        *result = std::make_shared<arrow::Decimal128Type>(type.precision, type.scale);
        break;
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    case TYPE_ARRAY: {
        DCHECK_EQ(type.children.size(), 1);
        std::shared_ptr<arrow::DataType> item_type;
        convert_to_arrow_type(type.children[0], &item_type);
        *result = std::make_shared<arrow::ListType>(item_type);
        break;
    }
    case TYPE_MAP: {
        DCHECK_EQ(type.children.size(), 2);
        std::shared_ptr<arrow::DataType> key_type;
        std::shared_ptr<arrow::DataType> val_type;
        convert_to_arrow_type(type.children[0], &key_type);
        convert_to_arrow_type(type.children[1], &val_type);
        *result = std::make_shared<arrow::MapType>(key_type, val_type);
        break;
    }
    case TYPE_STRUCT: {
        DCHECK_GT(type.children.size(), 0);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (size_t i = 0; i < type.children.size(); i++) {
            std::shared_ptr<arrow::DataType> field_type;
            convert_to_arrow_type(type.children[i], &field_type);
            fields.push_back(std::make_shared<arrow::Field>(type.field_names[i], field_type,
                                                            type.contains_nulls[i]));
        }
        *result = std::make_shared<arrow::StructType>(fields);
        break;
    }
    default:
        return Status::InvalidArgument("Unknown primitive type({})", type.type);
    }
    return Status::OK();
}

Status convert_to_arrow_field(SlotDescriptor* desc, std::shared_ptr<arrow::Field>* field) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(desc->type(), &type));
    *field = arrow::field(desc->col_name(), type, desc->is_nullable());
    return Status::OK();
}

Status convert_to_arrow_schema(const RowDescriptor& row_desc,
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

} // namespace doris
