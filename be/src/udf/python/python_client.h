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

/**
 * Base class for Python UDF/UDAF/UDTF clients
 * 
 * Provides common functionality for communicating with Python server via Arrow Flight:
 * - Connection management
 * - Stream initialization
 * - Error handling
 * - Process lifecycle management
 */
class PythonClient {
public:
    using FlightDescriptor = arrow::flight::FlightDescriptor;
    using FlightClient = arrow::flight::FlightClient;
    using FlightStreamWriter = arrow::flight::FlightStreamWriter;
    using FlightStreamReader = arrow::flight::FlightStreamReader;

    PythonClient() = default;
    virtual ~PythonClient() = default;

    /**
     * Initialize connection to Python server
     * @param func_meta Function metadata (contains client_type for operation name)
     * @param process Python process handle
     * @return Status
     */
    Status init(const PythonUDFMeta& func_meta, ProcessPtr process);

    /**
     * Close connection and cleanup resources
     * @return Status
     */
    Status close();

    /**
     * Handle Arrow Flight error
     * @param status Arrow status
     * @return Doris Status with formatted error message
     */
    Status handle_error(arrow::Status status);

    /**
     * Get process information for debugging
     * @return Process string representation
     */
    std::string print_process() const { return _process ? _process->to_string() : "null"; }

    /**
     * Get the underlying Python process
     * @return Process pointer
     */
    ProcessPtr get_process() const { return _process; }

protected:
    /**
     * Begin Flight stream with schema (called only once per stream)
     * @param schema Input schema
     * @return Status
     */
    Status begin_stream(const std::shared_ptr<arrow::Schema>& schema);

    /**
     * Write RecordBatch to server
     * @param input Input RecordBatch
     * @return Status
     */
    Status write_batch(const arrow::RecordBatch& input);

    /**
     * Read RecordBatch from server
     * @param output Output RecordBatch
     * @return Status
     */
    Status read_batch(std::shared_ptr<arrow::RecordBatch>* output);

    // Common state
    bool _inited = false;
    bool _begin = false;         // Track if Begin() has been called
    std::string _operation_name; // Operation name for error messages
    std::unique_ptr<FlightClient> _arrow_client;
    std::unique_ptr<FlightStreamWriter> _writer;
    std::unique_ptr<FlightStreamReader> _reader;
    ProcessPtr _process;

private:
    DISALLOW_COPY_AND_ASSIGN(PythonClient);
};

} // namespace doris
