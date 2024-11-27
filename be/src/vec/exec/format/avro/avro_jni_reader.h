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

#include <rapidjson/document.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "vec/exec/format/jni_reader.h"

namespace doris {
class RuntimeProfile;

class RuntimeState;

class SlotDescriptor;
namespace vectorized {
class Block;
} // namespace vectorized
struct TypeDescriptor;
} // namespace doris

namespace doris::vectorized {

/**
 * Read avro-format file
 */
class AvroJNIReader : public JniReader {
    ENABLE_FACTORY_CREATOR(AvroJNIReader);

public:
    /**
     * Call java side by jni to get table data.
     */
    AvroJNIReader(RuntimeState* state, RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const std::vector<SlotDescriptor*>& file_slot_descs, const TFileRangeDesc& range);

    /**
     * Call java side by jni to get table schema.
     */
    AvroJNIReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, const std::vector<SlotDescriptor*>& file_slot_descs);

    ~AvroJNIReader() override;

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status init_fetch_table_reader(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    TFileType::type get_file_type();

    Status init_fetch_table_schema_reader();

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override;

    TypeDescriptor convert_to_doris_type(const rapidjson::Value& column_schema);

private:
    const TFileScanRangeParams _params;
    const TFileRangeDesc _range;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range = nullptr;
};

} // namespace doris::vectorized
