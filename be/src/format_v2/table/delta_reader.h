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

#include "format_v2/table_reader.h"

namespace doris::format::delta {

// Owns Delta deletion-vector semantics while reusing TableReader's split lifecycle, column
// mapping and filtering, and the V2 ParquetReader for physical data files.
class DeltaTableReader final : public TableReader {
public:
    ENABLE_FACTORY_CREATOR(DeltaTableReader);
    ~DeltaTableReader() override = default;

protected:
    Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                       bool* has_delete_file) override;
    Status _load_deletion_vector(const TTableFormatFileDesc& t_desc, const DeleteFileDesc& desc,
                                 DeletionVector* deletion_vector) override;
    Status _decode_deletion_vector(const char* buf, size_t buffer_size, const DeleteFileDesc& desc,
                                   DeletionVector* deletion_vector) override;
};

} // namespace doris::format::delta
