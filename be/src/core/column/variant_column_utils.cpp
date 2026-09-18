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

#include "core/column/variant_column_utils.h"

#include "core/column/column_string.h"
#include "util/jsonb_document.h"

namespace doris {

size_t find_variant_sparse_path_lower_bound(StringRef path, const ColumnString& sparse_paths,
                                            size_t start, size_t end) {
    while (start < end) {
        const size_t middle = start + (end - start) / 2;
        if (sparse_paths.get_data_at(middle) < path) {
            start = middle + 1;
        } else {
            end = middle;
        }
    }
    return start;
}

bool is_variant_jsonb_value_semantically_empty(const JsonbValue* value) {
    if (value == nullptr || value->isNull()) {
        return true;
    }
    if (value->isArray()) {
        const auto* array = value->unpack<ArrayVal>();
        for (auto it = array->begin(); it != array->end(); ++it) {
            if (!is_variant_jsonb_value_semantically_empty(&*it)) {
                return false;
            }
        }
        return true;
    }
    if (value->isObject()) {
        const auto* object = value->unpack<ObjectVal>();
        for (auto it = object->begin(); it != object->end(); ++it) {
            if (!is_variant_jsonb_value_semantically_empty(it->value())) {
                return false;
            }
        }
        return true;
    }
    return false;
}

} // namespace doris
