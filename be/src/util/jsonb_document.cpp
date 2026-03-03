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
#include <string>
#include <vector>

#include "common/status.h"
#include "util/jsonb_writer.h"

namespace doris {

Status JsonbDocument::checkAndCreateDocument(const char* pb, size_t size, JsonbDocument** doc) {
    *doc = nullptr;
    if (!pb || size == 0) {
        static std::string buf = []() {
            JsonbWriter writer;
            (void)writer.writeNull();
            auto* out = writer.getOutput();
            return std::string(out->getBuffer(), out->getSize());
        }();
        // Treat empty input as a valid JSONB null document.
        *doc = reinterpret_cast<JsonbDocument*>(buf.data());
        return Status::OK();
    }
    if (!pb || size < sizeof(JsonbHeader) + sizeof(JsonbValue)) {
        return Status::InvalidArgument("Invalid JSONB document: too small size({}) or null pointer",
                                       size);
    }

    auto* doc_ptr = (JsonbDocument*)pb;
    if (doc_ptr->header_.ver_ != JSONB_VER) {
        return Status::InvalidArgument("Invalid JSONB document: invalid version({})",
                                       doc_ptr->header_.ver_);
    }

    auto* val = (JsonbValue*)doc_ptr->payload_;
    if (val->type() < JsonbType::T_Null || val->type() >= JsonbType::NUM_TYPES ||
        size != sizeof(JsonbHeader) + val->numPackedBytes()) {
        return Status::InvalidArgument("Invalid JSONB document: invalid type({}) or size({})",
                                       static_cast<JsonbTypeUnder>(val->type()), size);
    }

    *doc = doc_ptr;
    return Status::OK();
}

} // namespace doris