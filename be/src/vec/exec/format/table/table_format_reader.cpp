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

#include "table_format_reader.h"

#include <algorithm>
#include <string>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include <string>
#include "util/string_util.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
namespace TableSchemaChange {
    void func(const TSchemaInfoNode &table_id_to_info, const TSchemaInfoNode &file_id_to_info,  std::shared_ptr<tableNode> &root) {
        for (auto &[field_id, info]: table_id_to_info.children) {

            std::shared_ptr<tableNode> table_node = std::make_shared<tableNode>();
            if (file_id_to_info.children.contains(field_id)) {
                table_node->file_name = file_id_to_info.children.at(field_id).name;
                table_node->exists_in_file = true;
                func(info, file_id_to_info.children.at(field_id), table_node);
            } else {
                table_node->exists_in_file = false;
            }
            root->children.emplace(info.name, table_node);
        }
    }
}
Status TableSchemaChangeHelper::init_schema_info(
        const TSchemaInfoNode& table_id_to_name) {
    bool exist_schema = true;
    TSchemaInfoNode file_id_to_name;
    RETURN_IF_ERROR(get_file_col_id_to_name(exist_schema, file_id_to_name));
    if (!exist_schema) {
        // todo
    }

    std::cout  <<"TableSchemaChangeHelper\n";

    table_info_node_ptr = std::make_shared<TableSchemaChange::tableNode>();
    TableSchemaChange::func(table_id_to_name, file_id_to_name , table_info_node_ptr);

    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized