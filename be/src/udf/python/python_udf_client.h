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

#pragma once

#include <arrow/status.h>

#include "arrow/flight/client.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

class PythonUDFClient;
class PythonUDFProcessPool;

using PythonUDFClientPtr = std::shared_ptr<PythonUDFClient>;

class PythonUDFClient {
public:
    using FlightDescriptor = arrow::flight::FlightDescriptor;
    using FlightClient = arrow::flight::FlightClient;
    using FlightStreamWriter = arrow::flight::FlightStreamWriter;
    using FlightStreamReader = arrow::flight::FlightStreamReader;

    PythonUDFClient() = default;

    ~PythonUDFClient() = default;

    static Status create(const PythonUDFMeta& func_meta, ProcessPtr process,
                         PythonUDFClientPtr* client);

    Status init(const PythonUDFMeta& func_meta, ProcessPtr process);

    Status evaluate(const arrow::RecordBatch& input, std::shared_ptr<arrow::RecordBatch>* output);

    Status close();

    Status handle_error(arrow::Status status);

    std::string print_process() const { return _process->to_string(); }

private:
    DISALLOW_COPY_AND_ASSIGN(PythonUDFClient);

    bool _inited = false;
    bool _begin = false;
    std::unique_ptr<FlightClient> _arrow_client;
    std::unique_ptr<FlightStreamWriter> _writer;
    std::unique_ptr<FlightStreamReader> _reader;
    ProcessPtr _process;
};

} // namespace doris