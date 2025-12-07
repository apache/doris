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

#include <arrow/record_batch.h>
#include <cctz/time_zone.h>

#include "common/status.h"
#include "udf/python/python_udtf_client.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

/**
 * PythonUDTFFunction - Python User-Defined Table Function
 * 
 * Execution Flow:
 * 1. open() - Create Python UDTF client and establish RPC connection
 * 2. process_init(block) - Batch evaluate all rows:
 *    - Convert input block to Arrow RecordBatch
 *    - Call Python UDTF server via RPC (evaluates all rows in one call)
 *    - Receive Arrow ListArray (one list per input row)
 *    - Convert to array<T> column using DataTypeArraySerDe
 * 3. process_row(row_idx) - Set array offset for current row
 * 4. get_value()/get_same_many_values() - Extract values from array column
 * 5. process_close() - Clean up batch state (array column, offsets)
 * 6. close() - Close Python UDTF client and RPC connection
 */
class PythonUDTFFunction final : public TableFunction {
    ENABLE_FACTORY_CREATOR(PythonUDTFFunction);

public:
    PythonUDTFFunction(const TFunction& t_fn);
    ~PythonUDTFFunction() override = default;

    Status open() override;
    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_same_many_values(MutableColumnPtr& column, int length) override;
    int get_value(MutableColumnPtr& column, int max_step) override;
    Status close() override;

private:
    /**
     * Convert Python UDTF output (Arrow ListArray) to Doris array column
     * 
     * Input from Python server (via Arrow RPC):
     * - list_array: Arrow ListArray where each element corresponds to one input row's outputs
     *
     * Format:
     * - Single-column output: List<T> (e.g., List<int>, List<string>)
     * - Multi-column output: List<Struct<col1, col2, ...>>
     *
     * Example: 3 input rows producing variable output rows
     *   ListArray structure:
     *     [0]: [val1, val2, val3]       (3 elements)
     *     [1]: []                       (0 elements - empty array)
     *     [2]: [val4, val5, val6, val7] (4 elements)
     *
     * @param list_array Arrow ListArray containing UDTF output (length = num_input_rows)
     * @return Status indicating success or validation/conversion errors
     */
    Status _convert_list_array_to_array_column(const std::shared_ptr<arrow::ListArray>& list_array);

    const TFunction& _t_fn;
    DataTypePtr _return_type;
    PythonUDTFClientPtr _udtf_client;
    cctz::time_zone _timezone_obj;

    // Result storage (similar to Java UDTF)
    ColumnPtr _array_result_column;                // Array column storing all results
    ColumnArrayExecutionData _array_column_detail; // Array metadata for efficient access
    int64_t _array_offset = 0;                     // Offset into array for current row
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
