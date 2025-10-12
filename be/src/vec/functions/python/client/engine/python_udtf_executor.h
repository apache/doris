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

#include <vec/data_types/data_type.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "python_udf_client.h"
#include "vec/columns/column.h"

namespace doris::pyudf {
/**
 * The {@link PythonUdtfExecutor} is used to process column data by Python UDTF.
 * It sets up the Python environment, executes the UDTF, and manages the lifecycle of the UDTF execution.
 */
class PythonUdtfExecutor {
public:
    PythonUdtfExecutor() = default;

    PythonUdtfExecutor(const PythonUdtfExecutor&) = delete;

    PythonUdtfExecutor& operator=(const PythonUdtfExecutor&) = delete;

    /**
     * Sets up the Python UDTF environment.
     *
     * @param python_script_path the path to the Python script containing the UDTF.
     * @param entry_function_name the name of the entry function in the Python script.
     * @param null_on_failure whether output NULL column value if python error happens.
     * @return status.
     */
    Status setup(const std::string& python_script_path, const std::string& entry_function_name,
                 const bool& null_on_failure);

    /**
     * Init the Python UDTF on the provided input.
     *
     * @param udf_input_columns array of input columns for the UDF.
     * @param udf_input_column_number number of input columns.
     * @param row_index the index of input row in input columns.
     * @param udf_input_types data types of the input columns.
     * @param udf_output_column the output column where the result will be stored.
     * @param udf_output_type data type of the output column.
     * @return status.
     */
    Status init(vectorized::ColumnPtr udf_input_columns[], int udf_input_column_number,
                int row_index, const vectorized::DataTypes& udf_input_types,
                vectorized::ColumnPtr& udf_output_column,
                const vectorized::DataTypePtr& udf_output_type) const;

    /**
     * Fetch the next batch result of the Python UDTF.
     *
     * @param udf_output_column the output column where the result will be stored.
     * @param udf_output_type data type of the output column.
     * @return status.
     */
    Status fetch_next_batch_result(vectorized::ColumnPtr& udf_output_column,
                                   const vectorized::DataTypePtr& udf_output_type) const;

    /**
     * Closes the Python UDTF environment and cleans up resources.
     *
     * @return status.
     */
    Status close() const;

private:
    std::string python_script_path;
    std::string entry_function_name;
    mutable bool current_tf_is_null = false;
    mutable bool udtf_consumed = false;
    bool null_on_failure = false;
    mutable std::unique_ptr<PythonUdfClient> python_udf_client =
            std::make_unique<PythonUdfClient>();
};

} // namespace doris::pyudf
