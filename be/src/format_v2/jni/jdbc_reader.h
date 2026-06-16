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

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "format_v2/jni/jni_table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::jdbc {

class JdbcJniReader final : public format::JniTableReader {
public:
    ~JdbcJniReader() override = default;

    Status prepare_split(const format::SplitReadOptions& options) override;

protected:
    std::string connector_class() const override;
    Status build_scanner_params(std::map<std::string, std::string>* params) const override;
    Status build_jni_columns(
            std::vector<format::JniTableReader::JniColumn>* columns) const override;
    Status finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows) override;
    int64_t self_split_weight() const override;

private:
    bool _is_special_type(PrimitiveType type) const;
    std::string _replace_type_for(PrimitiveType type) const;
    DataTypePtr _transfer_type_for(const DataTypePtr& output_type) const;
    Status _cast_string_to_special_type(const format::JniTableReader::JniColumn& column,
                                        Block* jni_block, size_t jni_column_index,
                                        Block* output_block, size_t rows);

    std::map<std::string, std::string> _jdbc_params;
    TFileRangeDesc _current_range;
};

} // namespace doris::format::jdbc
