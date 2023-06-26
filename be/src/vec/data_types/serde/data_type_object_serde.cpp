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

#include "data_type_object_serde.h"

#include <rapidjson/stringbuffer.h>

#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"

namespace doris {

namespace vectorized {

Status DataTypeObjectSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    if (!variant.is_finalized()) {
        const_cast<ColumnObject&>(variant).finalize();
    }
    if (variant.is_scalar_variant()) {
        // Serialize scalar types, like int, string, array, faster path
        const auto& root = variant.get_subcolumn({});
        RETURN_IF_ERROR(root->get_least_common_type_serde()->write_column_to_mysql(
                root->get_finalized_column(), row_buffer, row_idx, col_const));
    } else {
        // Serialize hierarchy types to json format
        rapidjson::StringBuffer buffer;
        bool is_null = false;
        if (!variant.serialize_one_row_to_json_format(row_idx, &buffer, &is_null)) {
            return Status::InternalError("Invalid json format");
        }
        if (is_null) {
            row_buffer.push_null();
        } else {
            row_buffer.push_string(buffer.GetString(), buffer.GetLength());
        }
    }
    return Status::OK();
}

} // namespace vectorized

} // namespace doris