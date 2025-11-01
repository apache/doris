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

#include "udf/python/python_udf_client.h"

#include <utility>

#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

Status PythonUDFClient::create(const PythonUDFMeta& func_meta, ProcessPtr process,
                               PythonUDFClientPtr* client) {
    PythonUDFClientPtr python_udf_client = std::make_shared<PythonUDFClient>();
    RETURN_IF_ERROR(python_udf_client->init(func_meta, std::move(process)));
    *client = std::move(python_udf_client);
    return Status::OK();
}

Status PythonUDFClient::init(const PythonUDFMeta& func_meta, ProcessPtr process) {
    if (_inited) {
        return Status::InternalError("PythonUDFClient has already been initialized");
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

Status PythonUDFClient::evaluate(const arrow::RecordBatch& input,
                                 std::shared_ptr<arrow::RecordBatch>* output) {
    if (!_process->is_alive()) {
        return Status::RuntimeError("Python UDF process is not alive");
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

    // Step 3: Read response from server
    auto read_res = _reader->Next();
    if (!read_res.ok()) {
        return handle_error(read_res.status());
    }

    arrow::flight::FlightStreamChunk chunk = std::move(*read_res);
    if (!chunk.data) {
        _process->shutdown();
        return Status::InternalError("Received empty RecordBatch from Python UDF server");
    }
    *output = std::move(chunk.data);
    return Status::OK();
}

Status PythonUDFClient::handle_error(arrow::Status status) {
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

Status PythonUDFClient::close() {
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