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

#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "util/jsonb_writer.h"

namespace doris::json_format {

// One instance per transformer, used by one writer thread at a time. Retaining
// the type owners keeps cache keys alive. Do not put this in a global cache.
class JsonCellWriterState {
public:
    const DataTypeSerDe& serde(const DataTypePtr& type);
    JsonbWriter jsonb;

private:
    DorisMap<DataTypePtr, DataTypeSerDeSPtr> _serdes;
};

// Schema restrictions apply even to empty columns and NULL values. FILE exposes
// all six fields, with inline represented as padded Base64 or NULL.
Status validate_json_output_type(const DataTypePtr& type);

// The transformer must validate_json_output_type once per output schema, and
// validate_file_column once on each original input column/type (with ancestor
// null masks intact), before writing that block. Caller owns commit and must
// discard buffered output on any non-OK status.
Status write_json_cell(const IColumn& column, const DataTypePtr& type, size_t row,
                       BufferWritable& output, DataTypeSerDe::FormatOptions& options,
                       JsonCellWriterState& state);

} // namespace doris::json_format
