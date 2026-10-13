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
#include "core/column/column_nullable.h"
#include "util/jsonb_document.h"

namespace doris {

ColumnPtr StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::column_to_storage(const IColumn& column,
                                                                            size_t length) {
    const uint8_t* null_map = nullptr;
    const auto& strings = assert_cast<const ColumnString&>(peel_nullable(column, 0, &null_map));
    const size_t n = strings.size();
    if (strings.get_chars().size() == n * length) {
        return strings.get_ptr();
    }
    ColumnString::check_chars_length(n * length, n);
    auto stored = ColumnString::create();
    stored->get_chars().resize_fill(n * length, 0);
    auto& offsets = stored->get_offsets();
    offsets.resize(n);
    for (size_t i = 0; i < n; ++i) {
        offsets[i] = cast_set<ColumnString::Offset, size_t, false>((i + 1) * length);
        if (null_map != nullptr && null_map[i] != 0) {
            continue;
        }
        const StringRef value = strings.get_data_at(i);
        DCHECK_LE(value.size, length);
        memcpy(stored->get_chars().data() + i * length, value.data, value.size);
    }
    return stored;
}

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
