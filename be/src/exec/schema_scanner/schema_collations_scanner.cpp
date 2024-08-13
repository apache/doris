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

#include "exec/schema_scanner/schema_collations_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <string.h>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaCollationsScanner::_s_cols_columns = {
        //   name,       type,          size
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"IS_DEFAULT", TYPE_VARCHAR, sizeof(StringRef), false},
        {"IS_COMPILED", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SORTLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCollationsScanner::CollationStruct SchemaCollationsScanner::_s_collations[] = {
        {"utf8mb4_0900_bin", "utf8mb4", 309, "Yes", "Yes", 1},
        {nullptr, nullptr, 0, nullptr, nullptr, 0},
};

SchemaCollationsScanner::SchemaCollationsScanner()
        : SchemaScanner(_s_cols_columns, TSchemaTableType::SCH_COLLATIONS) {}

SchemaCollationsScanner::~SchemaCollationsScanner() {}

Status SchemaCollationsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaCollationsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto row_num = 0;
    while (nullptr != _s_collations[row_num].name) {
        ++row_num;
    }
    std::vector<void*> datas(row_num);

    // COLLATION_NAME
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_collations[i].name, strlen(_s_collations[i].name));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // charset
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_collations[i].charset, strlen(_s_collations[i].charset));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // id
    {
        std::vector<int64_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            srcs[i] = _s_collations[i].id;
            datas[i] = srcs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // is_default
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_collations[i].is_default, strlen(_s_collations[i].is_default));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // IS_COMPILED
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_collations[i].is_compile, strlen(_s_collations[i].is_compile));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // sortlen
    {
        std::vector<int64_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            srcs[i] = _s_collations[i].sortlen;
            datas[i] = srcs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }
    return Status::OK();
}

} // namespace doris
