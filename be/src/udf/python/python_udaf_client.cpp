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

#include "arrow/builder.h"
#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

// Unified Schema for ALL UDAF operations
// This ensures gRPC Flight Stream uses the same schema for all RecordBatches
// Fields: [operation: int8, place_id: int64, metadata: binary, data: binary]
// - operation: UDAFOperation enum value
// - place_id: Aggregate state identifier
// - metadata: Serialized metadata (e.g., is_single_place, row_start, row_end, place_offset)
// - data: Serialized operation-specific data (e.g., input RecordBatch, serialized_state)
static const std::shared_ptr<arrow::Schema> kUnifiedUDAFSchema = arrow::schema({
        arrow::field("operation", arrow::int8()),
        arrow::field("place_id", arrow::int64()),
        arrow::field("metadata", arrow::binary()),
        arrow::field("data", arrow::binary()),
});

// Metadata Schema for ACCUMULATE operation
// Fields: [is_single_place: bool, row_start: int64, row_end: int64, place_offset: int64]
static const std::shared_ptr<arrow::Schema> kAccumulateMetadataSchema = arrow::schema({
        arrow::field("is_single_place", arrow::boolean()),
        arrow::field("row_start", arrow::int64()),
        arrow::field("row_end", arrow::int64()),
        arrow::field("place_offset", arrow::int64()),
});

// Helper function to serialize RecordBatch to binary
static Status serialize_record_batch(const arrow::RecordBatch& batch,
                                     std::shared_ptr<arrow::Buffer>* out) {
    auto output_stream_result = arrow::io::BufferOutputStream::Create();
    if (UNLIKELY(!output_stream_result.ok())) {
        return Status::InternalError("Failed to create buffer output stream: {}",
                                     output_stream_result.status().message());
    }
    auto output_stream = std::move(output_stream_result).ValueOrDie();

    auto writer_result = arrow::ipc::MakeStreamWriter(output_stream, batch.schema());
    if (UNLIKELY(!writer_result.ok())) {
        return Status::InternalError("Failed to create IPC writer: {}",
                                     writer_result.status().message());
    }
    auto writer = std::move(writer_result).ValueOrDie();

    RETURN_DORIS_STATUS_IF_ERROR(writer->WriteRecordBatch(batch));
    RETURN_DORIS_STATUS_IF_ERROR(writer->Close());

    auto buffer_result = output_stream->Finish();
    if (UNLIKELY(!buffer_result.ok())) {
        return Status::InternalError("Failed to finish buffer: {}",
                                     buffer_result.status().message());
    }
    *out = std::move(buffer_result).ValueOrDie();
    return Status::OK();
}

// Helper function to deserialize RecordBatch from binary
static Status deserialize_record_batch(const std::shared_ptr<arrow::Buffer>& buffer,
                                       std::shared_ptr<arrow::RecordBatch>* out) {
    // Create BufferReader from the input buffer
    auto input_stream = std::make_shared<arrow::io::BufferReader>(buffer);

    // Open IPC stream reader
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input_stream);
    if (UNLIKELY(!reader_result.ok())) {
        return Status::InternalError("Failed to open IPC reader: {}",
                                     reader_result.status().message());
    }
    auto reader = std::move(reader_result).ValueOrDie();

    // Read the first (and only) RecordBatch
    auto batch_result = reader->Next();
    if (UNLIKELY(!batch_result.ok())) {
        return Status::InternalError("Failed to read RecordBatch: {}",
                                     batch_result.status().message());
    }

    *out = std::move(batch_result).ValueOrDie();
    if (UNLIKELY(!*out)) {
        return Status::InternalError("Deserialized RecordBatch is null");
    }

    return Status::OK();
}

// Helper function to validate and cast Arrow column to expected type
template <typename ArrowArrayType>
static Status validate_and_cast_column(const std::shared_ptr<arrow::RecordBatch>& batch,
                                       const std::string& operation_name,
                                       std::shared_ptr<ArrowArrayType>* out) {
    if (UNLIKELY(batch->num_columns() <= 0)) {
        return Status::InternalError("{} response: expected at least 1 column", operation_name);
    }

    auto column = batch->column(0);

    // Create expected type instance for comparison
    std::shared_ptr<arrow::DataType> expected_type;
    if constexpr (std::is_same_v<ArrowArrayType, arrow::BooleanArray>) {
        expected_type = arrow::boolean();
    } else if constexpr (std::is_same_v<ArrowArrayType, arrow::Int64Array>) {
        expected_type = arrow::int64();
    } else if constexpr (std::is_same_v<ArrowArrayType, arrow::BinaryArray>) {
        expected_type = arrow::binary();
    } else {
        return Status::InternalError("{} response: unsupported array type", operation_name);
    }

    if (UNLIKELY(!column->type()->Equals(expected_type))) {
        return Status::InternalError(
                "{} response: expected column 0 to be of type {}, but got type {}", operation_name,
                expected_type->ToString(), column->type()->ToString());
    }

    *out = std::static_pointer_cast<ArrowArrayType>(column);
    return Status::OK();
}

// Helper function to create a unified operation batch
static Status create_unified_batch(PythonUDAFClient::UDAFOperation operation, int64_t place_id,
                                   const std::shared_ptr<arrow::Buffer>& metadata,
                                   const std::shared_ptr<arrow::Buffer>& data,
                                   std::shared_ptr<arrow::RecordBatch>* out) {
    arrow::Int8Builder op_builder;
    arrow::Int64Builder place_builder;
    arrow::BinaryBuilder metadata_builder;
    arrow::BinaryBuilder data_builder;

    RETURN_DORIS_STATUS_IF_ERROR(op_builder.Append(static_cast<int8_t>(operation)));
    RETURN_DORIS_STATUS_IF_ERROR(place_builder.Append(place_id));

    if (metadata) {
        RETURN_DORIS_STATUS_IF_ERROR(
                metadata_builder.Append(metadata->data(), static_cast<int32_t>(metadata->size())));
    } else {
        RETURN_DORIS_STATUS_IF_ERROR(metadata_builder.AppendNull());
    }

    if (data) {
        RETURN_DORIS_STATUS_IF_ERROR(
                data_builder.Append(data->data(), static_cast<int32_t>(data->size())));
    } else {
        RETURN_DORIS_STATUS_IF_ERROR(data_builder.AppendNull());
    }

    std::shared_ptr<arrow::Array> op_array;
    std::shared_ptr<arrow::Array> place_array;
    std::shared_ptr<arrow::Array> metadata_array;
    std::shared_ptr<arrow::Array> data_array;

    RETURN_DORIS_STATUS_IF_ERROR(op_builder.Finish(&op_array));
    RETURN_DORIS_STATUS_IF_ERROR(place_builder.Finish(&place_array));
    RETURN_DORIS_STATUS_IF_ERROR(metadata_builder.Finish(&metadata_array));
    RETURN_DORIS_STATUS_IF_ERROR(data_builder.Finish(&data_array));

    *out = arrow::RecordBatch::Make(kUnifiedUDAFSchema, 1,
                                    {op_array, place_array, metadata_array, data_array});
    return Status::OK();
}

Status PythonUDAFClient::create(const PythonUDFMeta& func_meta, ProcessPtr process,
                                PythonUDAFClientPtr* client) {
    PythonUDAFClientPtr python_udaf_client = std::make_shared<PythonUDAFClient>();
    RETURN_IF_ERROR(python_udaf_client->init(func_meta, std::move(process)));
    *client = std::move(python_udaf_client);
    return Status::OK();
}

Status PythonUDAFClient::init(const PythonUDFMeta& func_meta, ProcessPtr process) {
    if (_inited) {
        return Status::InternalError("PythonUDAFClient has already been initialized");
    }

    arrow::flight::Location location;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(location,
                                        arrow::flight::Location::Parse(process->get_uri()));
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(_arrow_client, FlightClient::Connect(location));

    std::string command;
    RETURN_IF_ERROR(func_meta.serialize_to_json(&command));

    FlightDescriptor descriptor = FlightDescriptor::Command(command);
    arrow::flight::FlightClient::DoExchangeResult exchange_res;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(exchange_res, _arrow_client->DoExchange(descriptor));

    _reader = std::move(exchange_res.reader);
    _writer = std::move(exchange_res.writer);
    _process = std::move(process);
    _inited = true;

    return Status::OK();
}

Status PythonUDAFClient::create(int64_t place_id) {
    RETURN_IF_ERROR(
            (_execute_operation<UDAFOperation::CREATE, true>(place_id, nullptr, nullptr, nullptr)));
    _created_states.insert(place_id);
    return Status::OK();
}

Status PythonUDAFClient::accumulate(int64_t place_id, bool is_single_place,
                                    const arrow::RecordBatch& input, int64_t row_start,
                                    int64_t row_end, const int64_t* places, int64_t place_offset) {
    if (UNLIKELY(!_process->is_alive())) {
        return Status::RuntimeError("Python UDAF process is not alive");
    }

    // Validate input parameters
    if (UNLIKELY(row_start < 0 || row_end < row_start || row_end > input.num_rows())) {
        return Status::InvalidArgument(
                "Invalid row range: row_start={}, row_end={}, input.num_rows={}", row_start,
                row_end, input.num_rows());
    }

    // If there are multiple aggregate functions, places array is required
    if (UNLIKELY(!is_single_place && places == nullptr)) {
        return Status::InternalError(
                "places array must not be null when is_single_place=false (GROUP BY aggregation)");
    }

    // Build metadata RecordBatch
    // Schema: [is_single_place: bool, row_start: int64, row_end: int64, place_offset: int64]
    arrow::BooleanBuilder single_place_builder;
    arrow::Int64Builder row_start_builder;
    arrow::Int64Builder row_end_builder;
    arrow::Int64Builder offset_builder;

    RETURN_DORIS_STATUS_IF_ERROR(single_place_builder.Append(is_single_place));
    RETURN_DORIS_STATUS_IF_ERROR(row_start_builder.Append(row_start));
    RETURN_DORIS_STATUS_IF_ERROR(row_end_builder.Append(row_end));
    RETURN_DORIS_STATUS_IF_ERROR(offset_builder.Append(place_offset));

    std::shared_ptr<arrow::Array> single_place_array;
    std::shared_ptr<arrow::Array> row_start_array;
    std::shared_ptr<arrow::Array> row_end_array;
    std::shared_ptr<arrow::Array> offset_array;

    RETURN_DORIS_STATUS_IF_ERROR(single_place_builder.Finish(&single_place_array));
    RETURN_DORIS_STATUS_IF_ERROR(row_start_builder.Finish(&row_start_array));
    RETURN_DORIS_STATUS_IF_ERROR(row_end_builder.Finish(&row_end_array));
    RETURN_DORIS_STATUS_IF_ERROR(offset_builder.Finish(&offset_array));

    auto metadata_batch = arrow::RecordBatch::Make(
            kAccumulateMetadataSchema, 1,
            {single_place_array, row_start_array, row_end_array, offset_array});

    // Serialize metadata
    std::shared_ptr<arrow::Buffer> metadata_buffer;
    RETURN_IF_ERROR(serialize_record_batch(*metadata_batch, &metadata_buffer));

    // Build data RecordBatch (input columns + optional places array)
    std::vector<std::shared_ptr<arrow::Field>> data_fields;
    std::vector<std::shared_ptr<arrow::Array>> data_arrays;

    // Add input columns
    for (int i = 0; i < input.num_columns(); ++i) {
        data_fields.push_back(input.schema()->field(i));
        data_arrays.push_back(input.column(i));
    }

    // Add places array if in multi-place mode
    if (!is_single_place) {
        arrow::Int64Builder places_builder;
        for (int64_t i = 0; i < input.num_rows(); ++i) {
            RETURN_DORIS_STATUS_IF_ERROR(places_builder.Append(places[i]));
        }
        std::shared_ptr<arrow::Array> places_array;
        RETURN_DORIS_STATUS_IF_ERROR(places_builder.Finish(&places_array));

        data_fields.push_back(arrow::field("places", arrow::int64()));
        data_arrays.push_back(places_array);
    }

    auto data_schema = arrow::schema(data_fields);
    auto data_batch = arrow::RecordBatch::Make(data_schema, input.num_rows(), data_arrays);

    // Serialize data
    std::shared_ptr<arrow::Buffer> data_buffer;
    RETURN_IF_ERROR(serialize_record_batch(*data_batch, &data_buffer));

    // Create unified batch
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_IF_ERROR(create_unified_batch(UDAFOperation::ACCUMULATE, place_id, metadata_buffer,
                                         data_buffer, &batch));

    // Send to server and check rows_processed
    std::shared_ptr<arrow::RecordBatch> output;
    RETURN_IF_ERROR(_send_operation(batch.get(), &output));

    // Validate response: [rows_processed: int64]
    if (output->num_columns() != 1 || output->num_rows() != 1) {
        return Status::InternalError("Invalid ACCUMULATE response from Python UDAF server");
    }

    std::shared_ptr<arrow::Int64Array> int64_array;
    RETURN_IF_ERROR(validate_and_cast_column(output, "ACCUMULATE", &int64_array));

    int64_t rows_processed = int64_array->Value(0);
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
    // Execute SERIALIZE operation
    std::shared_ptr<arrow::RecordBatch> output;
    RETURN_IF_ERROR((_execute_operation<UDAFOperation::SERIALIZE, false>(place_id, nullptr, nullptr,
                                                                         &output)));

    // Extract serialized state from output (should be a binary column)
    if (output->num_columns() != 1 || output->num_rows() != 1) {
        return Status::InternalError("Invalid SERIALIZE response from Python UDAF server");
    }

    std::shared_ptr<arrow::BinaryArray> binary_array;
    RETURN_IF_ERROR(validate_and_cast_column(output, "SERIALIZE", &binary_array));

    int32_t length;
    const uint8_t* data = binary_array->GetValue(0, &length);

    // Check if serialization succeeded (non-empty binary)
    if (length == 0) {
        return Status::InternalError("SERIALIZE operation failed for place_id={}", place_id);
    }

    *serialized_state = arrow::Buffer::Wrap(data, length);

    return Status::OK();
}

Status PythonUDAFClient::merge(int64_t place_id,
                               const std::shared_ptr<arrow::Buffer>& serialized_state) {
    return _execute_operation<UDAFOperation::MERGE, true>(place_id, nullptr, serialized_state,
                                                          nullptr);
}

Status PythonUDAFClient::finalize(int64_t place_id, std::shared_ptr<arrow::RecordBatch>* output) {
    // Execute FINALIZE operation
    RETURN_IF_ERROR((_execute_operation<UDAFOperation::FINALIZE, false>(place_id, nullptr, nullptr,
                                                                        output)));

    // Validate basic response structure (but allow NULL values)
    if (!output || !(*output) || (*output)->num_columns() != 1 || (*output)->num_rows() != 1) {
        return Status::InternalError(
                "Invalid FINALIZE response: expected 1 column and 1 row, got {} columns and {} "
                "rows",
                output && (*output) ? (*output)->num_columns() : 0,
                output && (*output) ? (*output)->num_rows() : 0);
    }

    return Status::OK();
}

Status PythonUDAFClient::reset(int64_t place_id) {
    return _execute_operation<UDAFOperation::RESET, true>(place_id, nullptr, nullptr, nullptr);
}

Status PythonUDAFClient::destroy(int64_t place_id) {
    if (UNLIKELY(!_process->is_alive())) {
        return Status::RuntimeError("Python UDAF process is not alive");
    }

    // Create unified batch for DESTROY operation
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_IF_ERROR(
            create_unified_batch(UDAFOperation::DESTROY, place_id, nullptr, nullptr, &batch));

    // Send to server and check response
    std::shared_ptr<arrow::RecordBatch> output;
    RETURN_IF_ERROR(_send_operation(batch.get(), &output));

    // Validate response: [success: bool]
    if (output->num_columns() != 1 || output->num_rows() != 1) {
        return Status::InternalError("Invalid DESTROY response from Python UDAF server");
    }

    std::shared_ptr<arrow::BooleanArray> bool_array;
    RETURN_IF_ERROR(validate_and_cast_column(output, "DESTROY", &bool_array));

    if (!bool_array->Value(0)) {
        return Status::InternalError("DESTROY operation failed for place_id={}", place_id);
    }

    _created_states.erase(place_id);
    return Status::OK();
}

Status PythonUDAFClient::destroy_all() {
    // Destroy all tracked states
    for (int64_t place_id : _created_states) {
        // Ignore errors during cleanup
        static_cast<void>(destroy(place_id));
    }
    _created_states.clear();
    return Status::OK();
}

Status PythonUDAFClient::close() {
    if (!_inited || !_writer) return Status::OK();

    // Destroy all remaining states
    RETURN_IF_ERROR(destroy_all());

    auto writer_res = _writer->Close();
    if (!writer_res.ok()) {
        return _handle_error(writer_res);
    }

    _inited = false;
    _begin = false;
    _arrow_client.reset();
    _writer.reset();
    _reader.reset();

    if (auto* pool = _process->pool(); pool) {
        pool->return_process(std::move(_process));
    }

    return Status::OK();
}

std::string PythonUDAFClient::print_operation(UDAFOperation op) {
    switch (op) {
    case UDAFOperation::CREATE:
        return "CREATE";
    case UDAFOperation::ACCUMULATE:
        return "ACCUMULATE";
    case UDAFOperation::SERIALIZE:
        return "SERIALIZE";
    case UDAFOperation::MERGE:
        return "MERGE";
    case UDAFOperation::FINALIZE:
        return "FINALIZE";
    case UDAFOperation::RESET:
        return "RESET";
    case UDAFOperation::DESTROY:
        return "DESTROY";
    default:
        return "UNKNOWN";
    }
}

template <PythonUDAFClient::UDAFOperation operation, bool validate_response>
Status PythonUDAFClient::_execute_operation(int64_t place_id,
                                            const std::shared_ptr<arrow::Buffer>& metadata,
                                            const std::shared_ptr<arrow::Buffer>& data,
                                            std::shared_ptr<arrow::RecordBatch>* output) {
    if (UNLIKELY(!_process->is_alive())) {
        return Status::RuntimeError("Python UDAF process is not alive");
    }

    // Create unified batch for the operation
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_IF_ERROR(create_unified_batch(operation, place_id, metadata, data, &batch));

    // Send to server
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(_send_operation(batch.get(), &result));

    // Validate response if requested (compile-time branch)
    if constexpr (validate_response) {
        if (result->num_columns() != 1 || result->num_rows() != 1) {
            return Status::InternalError("Invalid {} response from Python UDAF server",
                                         print_operation(operation));
        }

        std::shared_ptr<arrow::BooleanArray> bool_array;
        RETURN_IF_ERROR(validate_and_cast_column(result, print_operation(operation), &bool_array));

        if (!bool_array->Value(0)) {
            return Status::InternalError("{} operation failed for place_id={}",
                                         print_operation(operation), place_id);
        }
    }

    // Set output if provided
    if (output != nullptr) {
        *output = std::move(result);
    }

    return Status::OK();
}

Status PythonUDAFClient::_send_operation(const arrow::RecordBatch* input,
                                         std::shared_ptr<arrow::RecordBatch>* output) {
    // CRITICAL: Lock here to protect Arrow Flight RPC operations (write/read)
    // Arrow Flight Client does NOT support concurrent read/write operations
    std::lock_guard<std::mutex> lock(_operation_mutex);

    // Step 1: Begin exchange with unified schema (only once, now protected by mutex)
    if (UNLIKELY(!_begin)) {
        // Always use the unified schema for all operations
        auto begin_res = _writer->Begin(kUnifiedUDAFSchema);
        if (!begin_res.ok()) {
            return _handle_error(begin_res);
        }
        _begin = true;
    }

    // Step 2: Write the record batch to server
    auto write_res = _writer->WriteRecordBatch(*input);
    if (!write_res.ok()) {
        return _handle_error(write_res);
    }

    // Step 3: Read response from server (if output is expected)
    if (output != nullptr) {
        auto read_res = _reader->Next();
        if (!read_res.ok()) {
            return _handle_error(read_res.status());
        }

        arrow::flight::FlightStreamChunk chunk = std::move(*read_res);
        if (!chunk.data) {
            _process->shutdown();
            return Status::InternalError("Received empty RecordBatch from Python UDAF server");
        }

        // The response is in unified format: [result_data: binary]
        // Extract and deserialize the actual result
        auto unified_response = chunk.data;
        if (unified_response->num_columns() != 1 || unified_response->num_rows() != 1) {
            return Status::InternalError(
                    "Invalid unified response format: expected 1 column and 1 row, got {} columns "
                    "and {} rows",
                    unified_response->num_columns(), unified_response->num_rows());
        }

        std::shared_ptr<arrow::BinaryArray> binary_array;
        RETURN_IF_ERROR(
                validate_and_cast_column(unified_response, "UNIFIED_RESPONSE", &binary_array));

        int32_t length;
        const uint8_t* data = binary_array->GetValue(0, &length);

        if (length == 0) {
            return Status::InternalError("Received empty result_data from Python UDAF server");
        }

        auto result_buffer = arrow::Buffer::Wrap(data, length);
        RETURN_IF_ERROR(deserialize_record_batch(result_buffer, output));
    }

    return Status::OK();
}

Status PythonUDAFClient::_handle_error(arrow::Status status) {
    DCHECK(!status.ok());
    _writer.reset();
    _reader.reset();
    _process->shutdown();

    std::string msg = status.message();
    // Remove Python traceback noise
    size_t pos = msg.find("The above exception was the direct cause");
    if (pos != std::string::npos) {
        msg = msg.substr(0, pos);
    }
    return Status::RuntimeError(trim(msg));
}

} // namespace doris
