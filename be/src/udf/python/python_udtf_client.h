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

#include <memory>

#include "arrow/flight/client.h"
#include "common/status.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"
#include "util/arrow/utils.h"

namespace doris {

class PythonUDTFClient;

using PythonUDTFClientPtr = std::shared_ptr<PythonUDTFClient>;

/**
 * Python UDTF Client
 * 
 * Implements simplified UDTF (User-Defined Table Function):
 * 
 * Handler Function:
 * - evaluate_func(*args): Process input arguments and yield output rows
 * 
 * UDTF Characteristics:
 * - Takes scalar or table inputs
 * - Returns table (multiple rows)
 * - Simple yield pattern
 * 
 * Example:
 * ```python
 * def evaluate_func(text, delimiter):
 *     # Split string by delimiter and return multiple results
 *     for item in text.split(delimiter):
 *         yield item
 * ```
 * 
 * SQL Usage:
 * ```sql
 * SELECT * FROM TABLE(split_string('a,b,c', ','));
 * ```
 * 
 * Communication protocol with Python server:
 * 1. Send input row batch to Python
 * 2. Python calls evaluate_func() for each input row
 * 3. Collect all output rows and return
 */
class PythonUDTFClient {
public:
    using FlightDescriptor = arrow::flight::FlightDescriptor;
    using FlightClient = arrow::flight::FlightClient;
    using FlightStreamWriter = arrow::flight::FlightStreamWriter;
    using FlightStreamReader = arrow::flight::FlightStreamReader;

    PythonUDTFClient() = default;
    ~PythonUDTFClient() = default;

    static Status create(const PythonUDFMeta& func_meta, ProcessPtr process,
                         PythonUDTFClientPtr* client);

    Status init(const PythonUDFMeta& func_meta, ProcessPtr process);

    /**
     * Evaluate UDTF on input rows
     * 
     * New Protocol (Offsets-based):
     * Python server returns a structured RecordBatch with 2 columns:
     * - Column 0 ("offsets"): Int64Array with N+1 elements marking boundaries
     * - Column 1 ("data"): Nested StructArray containing flattened output rows
     * 
     * Example:
     *   Input: 3 rows
     *   Output offsets: [0, 5, 7, 10]
     *   Output data: 10 rows (flattened)
     *   Meaning: Row 0 -> 5 outputs, Row 1 -> 2 outputs, Row 2 -> 3 outputs
     * 
     * @param input Input row batch (columns = UDTF function parameters)
     * @param offsets_array Output offsets array (N+1 elements for N input rows)
     * @param data_batch Output data batch (flattened output rows)
     * @return Status
     */
    Status evaluate(const arrow::RecordBatch& input,
                    std::shared_ptr<arrow::Int64Array>* offsets_array,
                    std::shared_ptr<arrow::RecordBatch>* data_batch);

    Status close();

    Status handle_error(arrow::Status status);

    std::string print_process() const { return _process->to_string(); }

private:
    DISALLOW_COPY_AND_ASSIGN(PythonUDTFClient);

    bool _inited = false;
    bool _begin = false;
    std::unique_ptr<FlightClient> _arrow_client;
    std::unique_ptr<FlightStreamWriter> _writer;
    std::unique_ptr<FlightStreamReader> _reader;
    ProcessPtr _process;
};

} // namespace doris
