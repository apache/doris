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

#include <memory>
#include <vector>

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
 * 1. open() - Create Python UDTF client and establish IPC connection
 * 2. process_init(block) - Batch evaluate all rows:
 *    - Convert input block to Arrow RecordBatch
 *    - Call Python UDTF server via IPC (evaluates all rows in one call)
 *    - Receive offsets array + flattened data batch
 *    - Convert to array<T> column for result storage
 * 3. process_row(row_idx) - Set array offset for current row
 * 4. get_value()/get_same_many_values() - Extract values from array column
 * 5. process_close() - Clean up batch state (array column, offsets)
 * 6. close() - Close Python UDTF client and IPC connection
 * 
 * Note: Unlike scalar UDFs that call Python per-row, UDTF uses batch processing
 *       for better performance. The array structure  allowsone-to-many mapping.
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
     * Convert Python UDTF output (offsets + flattened data) to Doris array column
     * 
     * Input from Python server (via Arrow IPC):
     * - offsets_array: Int64Array with (num_input_rows + 1) elements
     * - data_batch: RecordBatch containing flattened output rows (single column of type T)
     * 
     * Example: 3 input rows producing variable output rows
     *   offsets_array: [0, 3, 3, 7]
     *   data_batch: 7 rows of data (columns match element type T)
     * 
     * Output array column structure:
     *   Array[0] = data_batch[0:3]   (3 elements: rows 0-2)
     *   Array[1] = data_batch[3:3]   (0 elements: empty array)
     *   Array[2] = data_batch[3:7]   (4 elements: rows 3-6)
     * 
     * @param offsets_array Offsets defining output row ranges (length = num_input_rows + 1)
     * @param data_batch Flattened output data (single column, total rows = offsets[-1])
     * @param num_input_rows Number of input rows (and output arrays to create)
     * @return Status indicating success or validation/conversion errors
     */
    Status _convert_offsets_and_data_to_array_column(
            const std::shared_ptr<arrow::Int64Array>& offsets_array,
            const std::shared_ptr<arrow::RecordBatch>& data_batch,
            size_t num_input_rows);

    const TFunction& _t_fn;
    
    // Return type (wrapped as array<T> in constructor, like Java UDTF)
    DataTypePtr _return_type;

    // Python UDTF client for communication with Python server
    PythonUDTFClientPtr _udtf_client;

    // Result storage (similar to Java UDTF)
    ColumnPtr _array_result_column;                      // Array column storing all results
    ColumnArrayExecutionData _array_column_detail;       // Array metadata for efficient access
    int64_t _array_offset = 0;                           // Offset into array for current row

    // Timezone for Arrow conversion (default UTC)
    cctz::time_zone _timezone_obj;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
