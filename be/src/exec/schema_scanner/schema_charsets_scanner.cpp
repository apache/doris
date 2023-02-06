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

#include "common/status.h"
#include "vec/common/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaCharsetsScanner::_s_css_columns = {
        //   name,       type,          size
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DEFAULT_COLLATE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DESCRIPTION", TYPE_VARCHAR, sizeof(StringRef), false},
        {"MAXLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCharsetsScanner::CharsetStruct SchemaCharsetsScanner::_s_charsets[] = {
        {"utf8", "utf8_general_ci", "UTF-8 Unicode", 3},
        {nullptr, nullptr, nullptr, 0},
};

SchemaCharsetsScanner::SchemaCharsetsScanner()
        : SchemaScanner(_s_css_columns, TSchemaTableType::SCH_CHARSETS) {}

SchemaCharsetsScanner::~SchemaCharsetsScanner() {}

Status SchemaCharsetsScanner::get_next_block(vectorized::Block* block, bool* eos) {
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
    auto row_num = 0;
    while (nullptr != _s_charsets[row_num].charset) {
        ++row_num;
    }

    // variables names
    for (int i = 0; i < row_num; ++i) {
        StringRef str = StringRef(_s_charsets[i].charset, strlen(_s_charsets[i].charset));
        fill_dest_column(block, &str, _s_css_columns[0]);
    }
    // DEFAULT_COLLATE_NAME
    for (int i = 0; i < row_num; ++i) {
        StringRef str = StringRef(_s_charsets[i].default_collation,
                                  strlen(_s_charsets[i].default_collation));
        fill_dest_column(block, &str, _s_css_columns[1]);
    }
    // DESCRIPTION
    for (int i = 0; i < row_num; ++i) {
        StringRef str = StringRef(_s_charsets[i].description, strlen(_s_charsets[i].description));
        fill_dest_column(block, &str, _s_css_columns[2]);
    }
    // maxlen
    for (int i = 0; i < row_num; ++i) {
        int64_t src = _s_charsets[i].maxlen;
        fill_dest_column(block, &src, _s_css_columns[3]);
    }
    return Status::OK();
}

} // namespace doris
