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

#include "udf/python/python_client.h"

#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

Status PythonClient::init(const PythonUDFMeta& func_meta, ProcessPtr process) {
    if (_inited) {
        return Status::InternalError("PythonClient has already been initialized");
    }

    // Set operation name based on client type
    switch (func_meta.client_type) {
    case PythonClientType::UDF:
        _operation_name = "Python UDF";
        break;
    case PythonClientType::UDAF:
        _operation_name = "Python UDAF";
        break;
    case PythonClientType::UDTF:
        _operation_name = "Python UDTF";
        break;
    default:
        return Status::InternalError("Invalid Python client type");
    }

    // Parse and connect to Python server location
    arrow::flight::Location location;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(location,
                                        arrow::flight::Location::Parse(process->get_uri()));
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(_arrow_client, FlightClient::Connect(location));

    // Serialize function metadata to JSON command
    std::string command;
    RETURN_IF_ERROR(func_meta.serialize_to_json(&command));

    // Create Flight descriptor and establish bidirectional streaming
    FlightDescriptor descriptor = FlightDescriptor::Command(command);
    arrow::flight::FlightClient::DoExchangeResult exchange_res;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(exchange_res, _arrow_client->DoExchange(descriptor));

    _reader = std::move(exchange_res.reader);
    _writer = std::move(exchange_res.writer);
    _process = std::move(process);
    _inited = true;

    return Status::OK();
}

Status PythonClient::close() {
    if (!_inited || !_writer) {
        return Status::OK();
    }

    auto writer_res = _writer->Close();
    if (!writer_res.ok()) {
        // Don't propagate error from close, just log it
        LOG(WARNING) << "Error closing Python client writer: " << writer_res.message();
    }

    _inited = false;
    _begin = false;
    _arrow_client.reset();
    _writer.reset();
    _reader.reset();
    _process.reset();

    return Status::OK();
}

Status PythonClient::handle_error(arrow::Status status) {
    DCHECK(!status.ok());

    // Clean up resources
    _writer.reset();
    _reader.reset();

    // Extract and clean error message
    std::string msg = status.message();
    LOG(ERROR) << _operation_name << " error: " << msg;

    // Remove Python traceback noise for cleaner error messages
    size_t pos = msg.find("The above exception was the direct cause");
    if (pos != std::string::npos) {
        msg = msg.substr(0, pos);
    }

    return Status::RuntimeError(trim(msg));
}

Status PythonClient::begin_stream(const std::shared_ptr<arrow::Schema>& schema) {
    if (UNLIKELY(!_begin)) {
        auto begin_res = _writer->Begin(schema);
        if (!begin_res.ok()) {
            return handle_error(begin_res);
        }
        _begin = true;
    }
    return Status::OK();
}

Status PythonClient::write_batch(const arrow::RecordBatch& input) {
    auto write_res = _writer->WriteRecordBatch(input);
    if (!write_res.ok()) {
        return handle_error(write_res);
    }
    return Status::OK();
}

Status PythonClient::read_batch(std::shared_ptr<arrow::RecordBatch>* output) {
    auto read_res = _reader->Next();
    if (!read_res.ok()) {
        return handle_error(read_res.status());
    }

    arrow::flight::FlightStreamChunk chunk = std::move(*read_res);
    if (!chunk.data) {
        return Status::InternalError("Received null RecordBatch from {} server", _operation_name);
    }

    *output = std::move(chunk.data);
    return Status::OK();
}

} // namespace doris
