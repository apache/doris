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
#include <optional>

#include "format_v2/blob/paimon_blob_column_reader.h"
#include "format_v2/column_mapper.h"

namespace doris {
class Block;
}

namespace doris::format::paimon {

// Materializes one projected BLOB column using its file input and table output position.
// Column matching belongs to TableColumnMapper; readers interpret the input values.
class PaimonBlobColumnMaterializer {
public:
    // The reader is selected from the first BLOB split. Before that, this object
    // retains the projected column definition and table output position only.
    PaimonBlobColumnMaterializer(GlobalIndex table_output_position,
                                 ColumnDefinition column_definition,
                                 std::unique_ptr<PaimonBlobColumnReader> column_reader = nullptr);

    GlobalIndex table_output_position() const { return _table_output_position; }
    const ColumnDefinition& column_definition() const { return _column_definition; }

    Status prepare_split(const TPaimonFileDesc& split_params);
    Status prepare_file_input(const ColumnMapping& column_mapping,
                              const FileScanRequest& file_scan_request,
                              PaimonBlobInputLayout input_layout);
    Status materialize(const Block& file_block, Block* table_block,
                       const PaimonBlobReadContext& read_context);
    // Release split resources and input positions; retain the column definition and reader type.
    void reset();

private:
    GlobalIndex _table_output_position;
    ColumnDefinition _column_definition;
    std::unique_ptr<PaimonBlobColumnReader> _column_reader;

    // A position in the file-local Block, distinct from the table output position or a field ID.
    std::optional<LocalIndex> _file_input_position;
    DataTypePtr _input_column_type;
    bool _file_input_prepared = false;
};

} // namespace doris::format::paimon
