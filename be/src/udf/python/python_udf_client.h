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

class PythonUDFClient;

using PythonUDFClientPtr = std::shared_ptr<PythonUDFClient>;

/**
 * Python UDF Client
 *
 * Implements standard UDF (User-Defined Function) pattern with a single evaluation function:
 * - evaluate_func(*args): Process input arguments and return result
 *
 * UDF Characteristics:
 * - Takes scalar or column inputs
 * - Returns scalar or column outputs
 * - Stateless evaluation (each call is independent)
 * - Simple input-output transformation
 *
 * Example:
 * ```python
 * def evaluate_func(x, y):
 *     # Add two numbers
 *     return x + y
 * ```
 *
 * Communication protocol with Python server:
 * 1. Send input batch (RecordBatch with N rows)
 * 2. Python calls evaluate_func() for each row (or vectorized)
 * 3. Receive output batch (RecordBatch with N rows)
 */
class PythonUDFClient : public PythonClient {
public:
    PythonUDFClient() = default;
    ~PythonUDFClient() override = default;

    static Status create(const PythonUDFMeta& func_meta, ProcessPtr process,
                         PythonUDFClientPtr* client);

    /**
     * Evaluate UDF on input rows
     *
     * @param input Input row batch (columns = UDF function parameters)
     * @param output Output row batch (single column = UDF return value)
     * @return Status
     */
    Status evaluate(const arrow::RecordBatch& input, std::shared_ptr<arrow::RecordBatch>* output);

private:
    DISALLOW_COPY_AND_ASSIGN(PythonUDFClient);
};

} // namespace doris