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

#include "format_v2/blob/paimon_blob_column_reader.h"

#include <memory>
#include <vector>

#include "format_v2/blob/paimon_blob_descriptor_reader.h"
#include "format_v2/blob/paimon_blob_row_position_reader.h"

namespace doris::format::paimon {

// Selects the reader from the input layout; the output mode is independent.
std::unique_ptr<PaimonBlobColumnReader> PaimonBlobColumnReader::create(
        PaimonBlobInputLayout input_layout, const ColumnDefinition& column_definition,
        PaimonBlobOutputMode output_mode, PaimonBlobPayloadReader& payload_reader) {
    if (input_layout == PaimonBlobInputLayout::ROW_POSITION) {
        return std::make_unique<PaimonBlobRowPositionReader>(
                column_definition, std::vector<const TPaimonBlobFileDesc*> {}, output_mode);
    }
    return std::make_unique<PaimonBlobDescriptorReader>(column_definition, output_mode,
                                                        payload_reader);
}

} // namespace doris::format::paimon
