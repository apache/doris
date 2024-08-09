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

#include "exec/schema_scanner/schema_charsets_scanner.h"

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

std::vector<SchemaScanner::ColumnDesc> SchemaCharsetsScanner::_s_css_columns = {
        //   name,       type,          size
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DEFAULT_COLLATE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DESCRIPTION", TYPE_VARCHAR, sizeof(StringRef), false},
        {"MAXLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCharsetsScanner::CharsetStruct SchemaCharsetsScanner::_s_charsets[] = {
        {"utf8mb4", "utf8mb4_0900_bin", "UTF-8 Unicode", 4},
        {nullptr, nullptr, nullptr, 0},
};

SchemaCharsetsScanner::SchemaCharsetsScanner()
        : SchemaScanner(_s_css_columns, TSchemaTableType::SCH_CHARSETS) {}

SchemaCharsetsScanner::~SchemaCharsetsScanner() {}

Status SchemaCharsetsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaCharsetsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto row_num = 0;
    while (nullptr != _s_charsets[row_num].charset) {
        ++row_num;
    }
    std::vector<void*> datas(row_num);

    // variables names
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_charsets[i].charset, strlen(_s_charsets[i].charset));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // DEFAULT_COLLATE_NAME
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_charsets[i].default_collation,
                                strlen(_s_charsets[i].default_collation));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // DESCRIPTION
    {
        std::vector<StringRef> strs(row_num);
        for (int i = 0; i < row_num; ++i) {
            strs[i] = StringRef(_s_charsets[i].description, strlen(_s_charsets[i].description));
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // maxlen
    {
        std::vector<int64_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            srcs[i] = _s_charsets[i].maxlen;
            datas[i] = srcs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    return Status::OK();
}

} // namespace doris
