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
#include "vec/functions/python/client/data_types/data_type_converter.h"

namespace doris::pyudf {
/**
 * The {@link PythonUdfExecutor} is used to process column data by Python UDF.
 * It sets up the Python environment, executes the UDF, and manages the lifecycle of the UDF execution.
 */
class PythonUdfExecutor {
public:
    PythonUdfExecutor() = default;

    PythonUdfExecutor(const PythonUdfExecutor&) = delete;

    PythonUdfExecutor& operator=(const PythonUdfExecutor&) = delete;

    /**
     * Sets up the Python UDF environment.
     *
     * @param python_script_path the path to the Python script containing the UDF.
     * @param entry_python_filename the name of the entry Python script filename.
     * @param null_on_failure whether output NULL column value if python error happens.
     * @return status.
     */
    Status setup(const std::string& python_script_path, const std::string& entry_python_filename,
                 const bool& null_on_failure);

    /**
     * Executes the Python UDF on the provided input columns.
     *
     * @param udf_input_columns array of input columns for the UDF.
     * @param udf_input_column_number number of input columns.
     * @param udf_input_types data types of the input columns.
     * @param udf_output_column the output column where the result will be stored.
     * @param udf_output_type data type of the output column.
     * @param null_map map to record which result is null.
     * @return status.
     */
    Status execute(vectorized::ColumnPtr udf_input_columns[], int udf_input_column_number,
                   const vectorized::DataTypes& udf_input_types,
                   vectorized::ColumnPtr& udf_output_column,
                   const vectorized::DataTypePtr& udf_output_type,
                   vectorized::ColumnUInt8::MutablePtr& null_map) const;

    /**
     * Executes the Python UDF on the provided input columns.
     *
     * @param udf_input_columns array of input columns for the UDF.
     * @param udf_input_column_number number of input columns.
     * @param udf_input_types data types of the input columns.
     * @param batch_size batch size of the request that will be sent to pyudf.
     * @param udf_output_column the output column where the result will be stored.
     * @param udf_output_type data type of the output column.
     * @param null_map map to record which result is null.
     * @return status.
     */
    Status batch_execute(vectorized::ColumnPtr udf_input_columns[], int udf_input_column_number,
                         const vectorized::DataTypes& udf_input_types,
                         int batch_size,
                         vectorized::ColumnPtr& udf_output_column,
                         const vectorized::DataTypePtr& udf_output_type,
                         vectorized::ColumnUInt8::MutablePtr& null_map) const;

    /**
     * Closes the Python UDF environment and cleans up resources.
     *
     * @return status.
     */
    Status close() const;

private:
    Status batch_invoke_udf(std::vector<ScopedPyObject>& batch_reqs,
                            const std::unique_ptr<DataTypeConverter>& output_converter,
                            vectorized::ColumnPtr& udf_output_column,
                            const vectorized::DataTypePtr& udf_output_type,
                            vectorized::ColumnUInt8::MutablePtr& null_map) const;

    Status convert_batch_outputs(PyObject* output_obj,
                                 int batch_size,
                                 const std::unique_ptr<DataTypeConverter>& output_converter,
                                 vectorized::ColumnPtr& udf_output_column,
                                 const vectorized::DataTypePtr& udf_output_type,
                                 vectorized::ColumnUInt8::MutablePtr& null_map) const;

    std::string python_script_path;
    std::string entry_python_filename;
    mutable std::unique_ptr<PythonUdfClient> python_udf_client;
    bool null_on_failure = false;
};

} // namespace doris::pyudf
