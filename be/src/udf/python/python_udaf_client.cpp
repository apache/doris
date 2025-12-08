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

#include "udf/python/python_udaf_client.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "common/compiler_util.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

// Unified response structure for UDAF operations
// Arrow Schema: [success: bool, rows_processed: int64, data: binary]
// Different operations use different fields:
// - CREATE/MERGE/RESET/DESTROY: use success only
// - ACCUMULATE: use success + rows_processed (number of rows processed)
// - SERIALIZE: use success + data (serialized_state)
// - FINALIZE: use success + data (serialized result, may be null)
//
// This unified schema allows all operations to return consistent format,
// solving Arrow Flight's limitation that all responses must have the same schema.
static const std::shared_ptr<arrow::Schema> kUnifiedUDAFResponseSchema = arrow::schema({
        arrow::field("success", arrow::boolean()),
        arrow::field("rows_processed", arrow::int64()),
        arrow::field("serialized_data", arrow::binary()),
});

Status PythonUDAFClient::create(const PythonUDFMeta& func_meta, ProcessPtr process,
                                const std::shared_ptr<arrow::Schema>& data_schema,
                                PythonUDAFClientPtr* client) {
    PythonUDAFClientPtr python_udaf_client = std::make_shared<PythonUDAFClient>();
    RETURN_IF_ERROR(python_udaf_client->init(func_meta, std::move(process), data_schema));
    *client = std::move(python_udaf_client);
    return Status::OK();
}

Status PythonUDAFClient::init(const PythonUDFMeta& func_meta, ProcessPtr process,
                              const std::shared_ptr<arrow::Schema>& data_schema) {
    _schema = data_schema;
    return PythonClient::init(func_meta, std::move(process));
}

Status PythonUDAFClient::create(int64_t place_id) {
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_get_empty_request_batch(&request_batch));

    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::CREATE),
            .is_single_place = 0,
            .place_id = place_id,
            .row_start = 0,
            .row_end = 0,
    };

    std::shared_ptr<arrow::RecordBatch> response_batch;
    RETURN_IF_ERROR(_send_request(metadata, request_batch, &response_batch));

    // Parse unified response_batch: [success: bool, rows_processed: int64, serialized_data: binary]
    if (response_batch->num_rows() != 1) {
        return Status::InternalError("Invalid CREATE response_batch: expected 1 row");
    }

    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response_batch->column(0));
    if (!success_array->Value(0)) {
        return Status::InternalError("CREATE operation failed for place_id={}", place_id);
    }

    _created_place_id = place_id;
    return Status::OK();
}

Status PythonUDAFClient::accumulate(int64_t place_id, bool is_single_place,
                                    const arrow::RecordBatch& input, int64_t row_start,
                                    int64_t row_end) {
    // Validate input parameters
    if (UNLIKELY(row_start < 0 || row_end < row_start || row_end > input.num_rows())) {
        return Status::InvalidArgument(
                "Invalid row range: row_start={}, row_end={}, input.num_rows={}", row_start,
                row_end, input.num_rows());
    }

    // In multi-place mode, input RecordBatch must contain "places" column as last column
    if (UNLIKELY(!is_single_place &&
                 (input.num_columns() == 0 ||
                  input.schema()->field(input.num_columns() - 1)->name() != "places"))) {
        return Status::InternalError(
                "In multi-place mode, input RecordBatch must contain 'places' column as the "
                "last column");
    }

    // Create request batch: input data + NULL binary_data column
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_create_data_request_batch(input, &request_batch));

    // Create metadata structure
    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::ACCUMULATE),
            .is_single_place = static_cast<uint8_t>(is_single_place ? 1 : 0),
            .place_id = place_id,
            .row_start = row_start,
            .row_end = row_end,
    };

    // Send to server with metadata in app_metadata
    std::shared_ptr<arrow::RecordBatch> response;
    RETURN_IF_ERROR(_send_request(metadata, request_batch, &response));

    // Parse unified response: [success: bool, rows_processed: int64, serialized_data: binary]
    if (response->num_rows() != 1) {
        return Status::InternalError("Invalid ACCUMULATE response: expected 1 row");
    }

    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response->column(0));
    auto rows_processed_array = std::static_pointer_cast<arrow::Int64Array>(response->column(1));

    if (!success_array->Value(0)) {
        return Status::InternalError("ACCUMULATE operation failed for place_id={}", place_id);
    }

    // Cast to uint8_t* first to avoid UBSAN misaligned pointer errors
    const uint8_t* raw_ptr = reinterpret_cast<const uint8_t*>(rows_processed_array->raw_values());
    if (raw_ptr == nullptr) {
        return Status::InternalError("ACCUMULATE response has null rows_processed array");
    }
    int64_t rows_processed;
    memcpy(&rows_processed, raw_ptr, sizeof(int64_t));

    int64_t expected_rows = row_end - row_start;

    if (rows_processed < expected_rows) {
        return Status::InternalError(
                "ACCUMULATE operation only processed {} out of {} rows for place_id={}",
                rows_processed, expected_rows, place_id);
    }
    return Status::OK();
}

Status PythonUDAFClient::serialize(int64_t place_id,
                                   std::shared_ptr<arrow::Buffer>* serialized_state) {
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_get_empty_request_batch(&request_batch));

    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::SERIALIZE),
            .is_single_place = 0,
            .place_id = place_id,
            .row_start = 0,
            .row_end = 0,
    };

    std::shared_ptr<arrow::RecordBatch> response;
    RETURN_IF_ERROR(_send_request(metadata, request_batch, &response));

    // Parse unified response: [success: bool, rows_processed: int64, serialized_data: binary]
    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response->column(0));
    auto data_array = std::static_pointer_cast<arrow::BinaryArray>(response->column(2));

    if (!success_array->Value(0)) {
        return Status::InternalError("SERIALIZE operation failed for place_id={}", place_id);
    }

    // Cast to uint8_t* first to avoid UBSAN misaligned pointer errors
    const uint8_t* offsets = reinterpret_cast<const uint8_t*>(data_array->raw_value_offsets());
    if (offsets == nullptr) {
        return Status::InternalError("SERIALIZE response has null offsets");
    }
    int32_t offset_start, offset_end;
    memcpy(&offset_start, offsets, sizeof(int32_t));
    memcpy(&offset_end, offsets + sizeof(int32_t), sizeof(int32_t));

    int32_t length = offset_end - offset_start;

    if (length == 0) {
        return Status::InternalError("SERIALIZE operation returned empty data for place_id={}",
                                     place_id);
    }

    const uint8_t* data = data_array->value_data()->data() + offset_start;
    *serialized_state = arrow::Buffer::Wrap(data, length);
    return Status::OK();
}

Status PythonUDAFClient::merge(int64_t place_id,
                               const std::shared_ptr<arrow::Buffer>& serialized_state) {
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_create_binary_request_batch(serialized_state, &request_batch));

    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::MERGE),
            .is_single_place = 0,
            .place_id = place_id,
            .row_start = 0,
            .row_end = 0,
    };

    std::shared_ptr<arrow::RecordBatch> response;
    RETURN_IF_ERROR(_send_request(metadata, request_batch, &response));

    // Parse unified response: [success: bool, rows_processed: int64, serialized_data: binary]
    if (response->num_rows() != 1) {
        return Status::InternalError("Invalid MERGE response: expected 1 row");
    }

    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response->column(0));
    if (!success_array->Value(0)) {
        return Status::InternalError("MERGE operation failed for place_id={}", place_id);
    }

    return Status::OK();
}

Status PythonUDAFClient::finalize(int64_t place_id, std::shared_ptr<arrow::RecordBatch>* output) {
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_get_empty_request_batch(&request_batch));

    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::FINALIZE),
            .is_single_place = 0,
            .place_id = place_id,
            .row_start = 0,
            .row_end = 0,
    };

    std::shared_ptr<arrow::RecordBatch> response_batch;
    RETURN_IF_ERROR(_send_request(metadata, request_batch, &response_batch));

    // Parse unified response_batch: [success: bool, rows_processed: int64, serialized_data: binary]
    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response_batch->column(0));
    auto data_array = std::static_pointer_cast<arrow::BinaryArray>(response_batch->column(2));

    if (!success_array->Value(0)) {
        return Status::InternalError("FINALIZE operation failed for place_id={}", place_id);
    }

    // Cast to uint8_t* first to avoid UBSAN misaligned pointer errors
    const uint8_t* offsets = reinterpret_cast<const uint8_t*>(data_array->raw_value_offsets());
    if (offsets == nullptr) {
        return Status::InternalError("FINALIZE response has null offsets");
    }
    int32_t offset_start, offset_end;
    memcpy(&offset_start, offsets, sizeof(int32_t));
    memcpy(&offset_end, offsets + sizeof(int32_t), sizeof(int32_t));

    int32_t length = offset_end - offset_start;

    if (length == 0) {
        return Status::InternalError("FINALIZE operation returned empty data for place_id={}",
                                     place_id);
    }

    const uint8_t* data = data_array->value_data()->data() + offset_start;
    auto buffer = arrow::Buffer::Wrap(data, length);
    auto input_stream = std::make_shared<arrow::io::BufferReader>(buffer);

    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input_stream);
    if (UNLIKELY(!reader_result.ok())) {
        return Status::InternalError("Failed to deserialize FINALIZE result: {}",
                                     reader_result.status().message());
    }
    auto reader = std::move(reader_result).ValueOrDie();

    auto batch_result = reader->Next();
    if (UNLIKELY(!batch_result.ok())) {
        return Status::InternalError("Failed to read FINALIZE result: {}",
                                     batch_result.status().message());
    }

    *output = std::move(batch_result).ValueOrDie();

    return Status::OK();
}

Status PythonUDAFClient::reset(int64_t place_id) {
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_get_empty_request_batch(&request_batch));

    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::RESET),
            .is_single_place = 0,
            .place_id = place_id,
            .row_start = 0,
            .row_end = 0,
    };

    std::shared_ptr<arrow::RecordBatch> response;
    RETURN_IF_ERROR(_send_request(metadata, request_batch, &response));

    // Parse unified response: [success: bool, rows_processed: int64, serialized_data: binary]
    if (response->num_rows() != 1) {
        return Status::InternalError("Invalid RESET response: expected 1 row");
    }

    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response->column(0));
    if (!success_array->Value(0)) {
        return Status::InternalError("RESET operation failed for place_id={}", place_id);
    }

    return Status::OK();
}

Status PythonUDAFClient::destroy(int64_t place_id) {
    std::shared_ptr<arrow::RecordBatch> request_batch;
    RETURN_IF_ERROR(_get_empty_request_batch(&request_batch));

    UDAFMetadata metadata {
            .meta_version = UDAF_METADATA_VERSION,
            .operation = static_cast<uint8_t>(UDAFOperation::DESTROY),
            .is_single_place = 0,
            .place_id = place_id,
            .row_start = 0,
            .row_end = 0,
    };

    std::shared_ptr<arrow::RecordBatch> response;
    Status st = _send_request(metadata, request_batch, &response);

    // Always clear tracking, even if RPC failed
    _created_place_id.reset();

    if (!st.ok()) {
        LOG(WARNING) << "Failed to destroy place_id=" << place_id << ": " << st.to_string();
        return st;
    }

    // Parse unified response: [success: bool, rows_processed: int64, serialized_data: binary]
    if (response->num_rows() != 1) {
        return Status::InternalError("Invalid DESTROY response: expected 1 row");
    }

    auto success_array = std::static_pointer_cast<arrow::BooleanArray>(response->column(0));

    if (!success_array->Value(0)) {
        LOG(WARNING) << "DESTROY operation failed for place_id=" << place_id;
        return Status::InternalError("DESTROY operation failed for place_id={}", place_id);
    }

    return Status::OK();
}

Status PythonUDAFClient::close() {
    if (!_inited || !_writer) return Status::OK();

    // Destroy the place if it exists (cleanup on client destruction)
    if (_created_place_id.has_value()) {
        int64_t place_id = _created_place_id.value();
        Status st = destroy(place_id);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to destroy place_id=" << place_id
                         << " during close: " << st.to_string();
            // Clear tracking even on failure to prevent issues in base class close
            _created_place_id.reset();
        }
    }

    return PythonClient::close();
}

Status PythonUDAFClient::_send_request(const UDAFMetadata& metadata,
                                       const std::shared_ptr<arrow::RecordBatch>& request_batch,
                                       std::shared_ptr<arrow::RecordBatch>* response_batch) {
    DCHECK(response_batch != nullptr);

    // Create app_metadata buffer from metadata struct
    auto app_metadata =
            arrow::Buffer::Wrap(reinterpret_cast<const uint8_t*>(&metadata), sizeof(metadata));

    std::lock_guard<std::mutex> lock(_operation_mutex);

    // Check if writer/reader are still valid (they could be reset by handle_error)
    if (UNLIKELY(!_writer || !_reader)) {
        return Status::InternalError("{} writer/reader have been closed due to previous error",
                                     _operation_name);
    }

    // Begin stream on first call (using data schema: argument_types + places + binary_data)
    if (UNLIKELY(!_begin)) {
        auto begin_res = _writer->Begin(_schema);
        if (!begin_res.ok()) {
            return handle_error(begin_res);
        }
        _begin = true;
    }

    // Write batch with metadata in app_metadata
    auto write_res = _writer->WriteWithMetadata(*request_batch, app_metadata);
    if (!write_res.ok()) {
        return handle_error(write_res);
    }

    // Read unified response: [success: bool, rows_processed: int64, serialized_data: binary]
    auto read_res = _reader->Next();
    if (!read_res.ok()) {
        return handle_error(read_res.status());
    }

    arrow::flight::FlightStreamChunk chunk = std::move(*read_res);
    if (!chunk.data) {
        return Status::InternalError("Received empty RecordBatch from {} server", _operation_name);
    }

    // Validate unified response schema
    if (!chunk.data->schema()->Equals(kUnifiedUDAFResponseSchema)) {
        return Status::InternalError(
                "Invalid response schema: expected [success: bool, rows_processed: int64, "
                "serialized_data: binary], got {}",
                chunk.data->schema()->ToString());
    }

    *response_batch = std::move(chunk.data);
    return Status::OK();
}

Status PythonUDAFClient::_create_data_request_batch(const arrow::RecordBatch& input_data,
                                                    std::shared_ptr<arrow::RecordBatch>* out) {
    // Determine if input has places column
    int num_input_columns = input_data.num_columns();
    bool has_places = false;
    if (num_input_columns > 0 &&
        input_data.schema()->field(num_input_columns - 1)->name() == "places") {
        has_places = true;
    }

    // Expected schema structure: [argument_types..., places, binary_data]
    // - Input in single-place mode: [argument_types...]
    // - Input in multi-place mode: [argument_types..., places]
    std::vector<std::shared_ptr<arrow::Array>> columns;
    // Copy argument_types columns
    int num_arg_columns = has_places ? (num_input_columns - 1) : num_input_columns;

    for (int i = 0; i < num_arg_columns; ++i) {
        columns.push_back(input_data.column(i));
    }

    // Add places column
    if (has_places) {
        // Use existing places column from input
        columns.push_back(input_data.column(num_input_columns - 1));
    } else {
        // Create NULL places column for single-place mode
        arrow::Int64Builder places_builder;
        std::shared_ptr<arrow::Array> places_array;
        RETURN_DORIS_STATUS_IF_ERROR(places_builder.AppendNulls(input_data.num_rows()));
        RETURN_DORIS_STATUS_IF_ERROR(places_builder.Finish(&places_array));
        columns.push_back(places_array);
    }

    // Add NULL binary_data column
    arrow::BinaryBuilder binary_builder;
    std::shared_ptr<arrow::Array> binary_array;
    RETURN_DORIS_STATUS_IF_ERROR(binary_builder.AppendNulls(input_data.num_rows()));
    RETURN_DORIS_STATUS_IF_ERROR(binary_builder.Finish(&binary_array));
    columns.push_back(binary_array);

    *out = arrow::RecordBatch::Make(_schema, input_data.num_rows(), columns);
    return Status::OK();
}

Status PythonUDAFClient::_create_binary_request_batch(
        const std::shared_ptr<arrow::Buffer>& binary_data,
        std::shared_ptr<arrow::RecordBatch>* out) {
    std::vector<std::shared_ptr<arrow::Array>> columns;

    // Create NULL arrays for data columns (all columns except the last binary_data column)
    // Schema: [argument_types..., places, binary_data]
    int num_data_columns = _schema->num_fields() - 1;
    for (int i = 0; i < num_data_columns; ++i) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        std::shared_ptr<arrow::Array> null_array;
        RETURN_DORIS_STATUS_IF_ERROR(arrow::MakeBuilder(arrow::default_memory_pool(),
                                                        _schema->field(i)->type(), &builder));
        RETURN_DORIS_STATUS_IF_ERROR(builder->AppendNull());
        RETURN_DORIS_STATUS_IF_ERROR(builder->Finish(&null_array));
        columns.push_back(null_array);
    }

    // Create binary_data column
    arrow::BinaryBuilder binary_builder;
    std::shared_ptr<arrow::Array> binary_array;
    RETURN_DORIS_STATUS_IF_ERROR(
            binary_builder.Append(binary_data->data(), static_cast<int32_t>(binary_data->size())));
    RETURN_DORIS_STATUS_IF_ERROR(binary_builder.Finish(&binary_array));
    columns.push_back(binary_array);

    *out = arrow::RecordBatch::Make(_schema, 1, columns);
    return Status::OK();
}

Status PythonUDAFClient::_get_empty_request_batch(std::shared_ptr<arrow::RecordBatch>* out) {
    // Return cached batch if already created
    if (_empty_request_batch) {
        *out = _empty_request_batch;
        return Status::OK();
    }

    // Create empty batch on first use (all columns NULL, 1 row)
    std::vector<std::shared_ptr<arrow::Array>> columns;

    for (int i = 0; i < _schema->num_fields(); ++i) {
        auto field = _schema->field(i);
        std::unique_ptr<arrow::ArrayBuilder> builder;
        std::shared_ptr<arrow::Array> null_array;
        RETURN_DORIS_STATUS_IF_ERROR(
                arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
        RETURN_DORIS_STATUS_IF_ERROR(builder->AppendNull());
        RETURN_DORIS_STATUS_IF_ERROR(builder->Finish(&null_array));
        columns.push_back(null_array);
    }

    _empty_request_batch = arrow::RecordBatch::Make(_schema, 1, columns);
    *out = _empty_request_batch;
    return Status::OK();
}

} // namespace doris
