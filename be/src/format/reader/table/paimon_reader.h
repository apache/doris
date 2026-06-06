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

#include "format/reader/table_reader.h"

namespace doris {
struct DeleteFileDesc;
}
namespace doris::paimon {

class PaimonReader final : public reader::TableReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonReader);
    ~PaimonReader() final = default;
    Status prepare_split(const reader::SplitReadOptions& options) override;

protected:
    reader::TableColumnMappingMode mapping_mode() const override;
    Status annotate_file_schema(std::vector<reader::ColumnDefinition>* file_schema) override;

    Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                       bool* has_delete_file) override;

private:
    int64_t _split_schema_id = -1;
};

} // namespace doris::paimon
