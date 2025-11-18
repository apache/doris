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

#include "udf/python/python_udtf_client.h"

#include <sstream>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/array_primitive.h"
#include "arrow/builder.h"
#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

Status PythonUDTFClient::create(const PythonUDFMeta& func_meta, ProcessPtr process,
                                PythonUDTFClientPtr* client) {
    PythonUDTFClientPtr python_udtf_client = std::make_shared<PythonUDTFClient>();
    RETURN_IF_ERROR(python_udtf_client->init(func_meta, std::move(process)));
    *client = std::move(python_udtf_client);
    return Status::OK();
}

Status PythonUDTFClient::init(const PythonUDFMeta& func_meta, ProcessPtr process) {
    if (_inited) {
        return Status::InternalError("PythonUDTFClient has already been initialized");
    }

    // Connect to Python UDF server via Flight RPC
    arrow::flight::Location location;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(location,
                                        arrow::flight::Location::Parse(process->get_uri()));
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(_arrow_client, FlightClient::Connect(location));

    // Create Flight descriptor with function metadata
    std::string command;
    RETURN_IF_ERROR(func_meta.serialize_to_json(&command));
    FlightDescriptor descriptor = FlightDescriptor::Command(command);

    // Establish bidirectional streaming
    arrow::flight::FlightClient::DoExchangeResult exchange_res;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(exchange_res, _arrow_client->DoExchange(descriptor));
    _reader = std::move(exchange_res.reader);
    _writer = std::move(exchange_res.writer);

    _process = std::move(process);
    _inited = true;
    return Status::OK();
}

Status PythonUDTFClient::evaluate(const arrow::RecordBatch& input,
                                  std::shared_ptr<arrow::Int64Array>* offsets_array,
                                  std::shared_ptr<arrow::RecordBatch>* data_batch) {
    if (!_process->is_alive()) {
        return Status::RuntimeError("Python UDTF process is not alive");
    }

    // Step 1: Begin exchange with schema (only once)
    if (UNLIKELY(!_begin)) {
        auto begin_res = _writer->Begin(input.schema());
        if (!begin_res.ok()) {
            return handle_error(begin_res);
        }
        _begin = true;
    }

    // Step 2: Write the record batch to server
    auto write_res = _writer->WriteRecordBatch(input);
    if (!write_res.ok()) {
        return handle_error(write_res);
    }

    // Step 3: Read the structured response with offsets + data
    auto read_res = _reader->Next();
    if (!read_res.ok()) {
        return handle_error(read_res.status());
    }

    arrow::flight::FlightStreamChunk chunk = std::move(*read_res);
    if (!chunk.data) {
        _process->shutdown();
        return Status::InternalError("Received null RecordBatch from Python UDTF server");
    }

    // Step 4: Parse the structured response
    // Offsets are stored in RecordBatch metadata, data is in columns
    auto response_batch = chunk.data;
    
    if (!response_batch->schema()->metadata()) {
        return Status::InternalError("Invalid UDTF response: missing metadata with offsets");
    }
    
    auto metadata = response_batch->schema()->metadata();
    
    // Extract offsets from metadata
    int offsets_index = metadata->FindKey("offsets");
    if (offsets_index < 0) {
        return Status::InternalError("Invalid UDTF response: metadata missing 'offsets' key");
    }
    
    std::string offsets_str = metadata->value(offsets_index);
    
    // Parse comma-separated offsets
    std::vector<int64_t> offsets_vec;
    std::stringstream ss(offsets_str);
    std::string token;
    while (std::getline(ss, token, ',')) {
        offsets_vec.push_back(std::stoll(token));
    }
    
    // Build offsets array
    arrow::Int64Builder offsets_builder;
    RETURN_DORIS_STATUS_IF_ERROR(offsets_builder.AppendValues(offsets_vec));
    auto offsets_result = offsets_builder.Finish();
    if (!offsets_result.ok()) {
        return handle_error(offsets_result.status());
    }
    *offsets_array = std::static_pointer_cast<arrow::Int64Array>(std::move(offsets_result).ValueOrDie());
    
    // Data batch is the response_batch itself
    // Remove metadata from schema to avoid carrying it forward
    auto clean_schema = response_batch->schema()->WithMetadata(nullptr);
    *data_batch = arrow::RecordBatch::Make(clean_schema, response_batch->num_rows(),
                                           response_batch->columns());

    return Status::OK();
}

Status PythonUDTFClient::handle_error(arrow::Status status) {
    DCHECK(!status.ok());
    _writer.reset();
    _reader.reset();
    _process->shutdown();
    std::string msg = status.message();
    size_t pos = msg.find("The above exception was the direct cause");
    if (pos != std::string::npos) {
        msg = msg.substr(0, pos);
    }
    return Status::RuntimeError(trim(msg));
}

Status PythonUDTFClient::close() {
    if (!_inited || !_writer) return Status::OK();

    auto writer_res = _writer->Close();
    if (!writer_res.ok()) {
        return handle_error(writer_res);
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

} // namespace doris
