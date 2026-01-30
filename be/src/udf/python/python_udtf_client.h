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

#include "udf/python/python_client.h"

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
 *         # or yield (item, )
 *         yield item 
 * ```
 * 
 * Communication protocol with Python server:
 * 1. Send input row batch to Python
 * 2. Python calls evaluate_func() for each input row
 * 3. Collect all output rows and return
 */
class PythonUDTFClient : public PythonClient {
public:
    PythonUDTFClient() = default;
    ~PythonUDTFClient() override = default;

    static Status create(const PythonUDFMeta& func_meta, ProcessPtr process,
                         PythonUDTFClientPtr* client);

    /**
     * Evaluate UDTF on input rows
     * 
     * Protocol (ListArray-based):
     * Python server returns a RecordBatch with 1 column:
     * - Column 0: ListArray where each list element corresponds to one input row's outputs
     * 
     * Example:
     *   Input: 3 rows
     *   Output ListArray:
     *     [0]: [val1, val2, val3]      (3 elements for input row 0)
     *     [1]: []                       (0 elements for input row 1)
     *     [2]: [val4, val5, val6, val7] (4 elements for input row 2)
     * 
     * @param input Input row batch (columns = UDTF function parameters)
     * @param list_array Output ListArray (length = num_input_rows)
     * @return Status
     */
    Status evaluate(const arrow::RecordBatch& input, std::shared_ptr<arrow::ListArray>* list_array);

private:
    DISALLOW_COPY_AND_ASSIGN(PythonUDTFClient);
};

} // namespace doris
