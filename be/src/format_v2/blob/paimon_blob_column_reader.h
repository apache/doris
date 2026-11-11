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

#include <memory>
#include <string>

#include "format_v2/blob/paimon_blob_column_builder.h"
#include "format_v2/blob/paimon_blob_read_context.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::paimon {

class PaimonBlobPayloadReader;

enum class PaimonBlobInputLayout { ROW_POSITION, DESCRIPTOR_COLUMN };

// One call per column/batch. Concrete readers own input interpretation; all of them
// use PaimonBlobColumnBuilder for FILE construction. No file/table Block positions here.
class PaimonBlobColumnReader {
public:
    using ReadContext = PaimonBlobReadContext;
    virtual ~PaimonBlobColumnReader() = default;

    static std::unique_ptr<PaimonBlobColumnReader> create(PaimonBlobInputLayout input_layout,
                                                          const ColumnDefinition& column_definition,
                                                          PaimonBlobOutputMode output_mode,
                                                          PaimonBlobPayloadReader& payload_reader);

    virtual Status prepare_split(const TPaimonFileDesc& split_params) = 0;
    // Release current split state; retain the column definition and output mode for reuse.
    virtual void reset() = 0;
    virtual Status read(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                        const ReadContext& read_context, ColumnPtr* result_column) = 0;

protected:
    PaimonBlobColumnReader(const ColumnDefinition& column_definition,
                           PaimonBlobOutputMode output_mode)
            : _column_name(column_definition.name),
              _result_column_type(column_definition.type),
              _output_mode(output_mode) {}

    std::string _column_name;
    DataTypePtr _result_column_type;
    PaimonBlobOutputMode _output_mode;
};

} // namespace doris::format::paimon
