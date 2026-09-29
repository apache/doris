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

#include "storage/storage_layout.h"

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "util/jsonb_document.h"

namespace doris {

Status admit_storage_rows(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                          const uint8_t* null_map) {
    if (type != FieldType::OLAP_FIELD_TYPE_STRING && type != FieldType::OLAP_FIELD_TYPE_JSONB) {
        return Status::OK();
    }
    const auto& strings = assert_cast<const ColumnString&>(column);
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            continue;
        }
        const StringRef value = strings.get_data_at(row_pos + i);
        if (UNLIKELY(value.size > config::string_type_length_soft_limit_bytes)) {
            return Status::NotSupported(
                    "Not support string len over than `string_type_length_soft_limit_bytes`"
                    " in vec engine.");
        }
        // Make sure that the json binary data written in is the correct jsonb value.
        if (type == FieldType::OLAP_FIELD_TYPE_JSONB) {
            const JsonbDocument* doc = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(value.data, value.size, &doc));
        }
    }
    return Status::OK();
}

} // namespace doris
