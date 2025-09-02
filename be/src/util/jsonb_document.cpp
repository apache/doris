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

#include "jsonb_document.h"

#include <memory>
#include <vector>

#include "common/status.h"
#include "util/jsonb_writer.h"

namespace doris {
JsonbFindResult JsonbValue::findValue(JsonbPath& path) const {
    JsonbFindResult result;
    bool is_wildcard = false;

    std::vector<const JsonbValue*> values;
    std::vector<const JsonbValue*> results;
    results.emplace_back(this);

    if (path.is_supper_wildcard()) {
        std::function<void(const JsonbValue*)> foreach_values;
        foreach_values = [&](const JsonbValue* val) {
            if (val->isObject()) {
                for (const auto& it : *val->unpack<ObjectVal>()) {
                    results.emplace_back(it.value());
                    foreach_values(it.value());
                }
            } else if (val->isArray()) {
                for (const auto& it : *val->unpack<ArrayVal>()) {
                    results.emplace_back(&it);
                    foreach_values(&it);
                }
            }
        };
        is_wildcard = true;
        foreach_values(this);
    }

    for (size_t i = 0; i < path.get_leg_vector_size(); ++i) {
        values.assign(results.begin(), results.end());
        results.clear();
        for (const auto* pval : values) {
            switch (path.get_leg_from_leg_vector(i)->type) {
            case MEMBER_CODE: {
                if (LIKELY(pval->type == JsonbType::T_Object)) {
                    if (path.get_leg_from_leg_vector(i)->leg_len == 1 &&
                        *path.get_leg_from_leg_vector(i)->leg_ptr == WILDCARD) {
                        is_wildcard = true;
                        for (const auto& it : *pval->unpack<ObjectVal>()) {
                            results.emplace_back(it.value());
                        }
                        continue;
                    }

                    pval = pval->unpack<ObjectVal>()->find(path.get_leg_from_leg_vector(i)->leg_ptr,
                                                           path.get_leg_from_leg_vector(i)->leg_len,
                                                           nullptr);

                    if (pval) {
                        results.emplace_back(pval);
                    }
                }
                continue;
            }
            case ARRAY_CODE: {
                if (path.get_leg_from_leg_vector(i)->leg_len == 1 &&
                    *path.get_leg_from_leg_vector(i)->leg_ptr == WILDCARD) {
                    if (LIKELY(pval->type == JsonbType::T_Array)) {
                        is_wildcard = true;
                        for (const auto& it : *pval->unpack<ArrayVal>()) {
                            results.emplace_back(&it);
                        }
                    }
                    continue;
                }

                if (pval->type != JsonbType::T_Array &&
                    path.get_leg_from_leg_vector(i)->array_index == 0) {
                    // Same as mysql and postgres
                    results.emplace_back(pval);
                    continue;
                }

                if (pval->type != JsonbType::T_Array ||
                    path.get_leg_from_leg_vector(i)->leg_ptr != nullptr ||
                    path.get_leg_from_leg_vector(i)->leg_len != 0) {
                    continue;
                }

                if (path.get_leg_from_leg_vector(i)->array_index >= 0) {
                    pval = pval->unpack<ArrayVal>()->get(
                            path.get_leg_from_leg_vector(i)->array_index);
                } else {
                    pval = pval->unpack<ArrayVal>()->get(
                            pval->unpack<ArrayVal>()->numElem() +
                            path.get_leg_from_leg_vector(i)->array_index);
                }

                if (pval) {
                    results.emplace_back(pval);
                }
                continue;
            }
            }
        }
    }

    if (is_wildcard) {
        result.is_wildcard = true;
        if (results.empty()) {
            result.value = nullptr; // No values found
        } else {
            result.writer = std::make_unique<JsonbWriter>();
            result.writer->writeStartArray();
            for (const auto* pval : results) {
                result.writer->writeValue(pval);
            }
            result.writer->writeEndArray();

            JsonbDocument* doc = nullptr;
            THROW_IF_ERROR(JsonbDocument::checkAndCreateDocument(
                    result.writer->getOutput()->getBuffer(), result.writer->getOutput()->getSize(),
                    &doc));
            result.value = doc->getValue();
        }
    } else if (results.size() == 1) {
        result.value = results[0];
    }

    return result;
}
} // namespace doris