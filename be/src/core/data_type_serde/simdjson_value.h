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

#pragma once

#include <simdjson.h>

#include <string>
#include <string_view>
#include <unordered_map>

#include "core/data_type_serde/data_type_serde.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_writer.h"

namespace doris {
// Borrowed single-pass value. This view keeps simdjson implementation namespaces and SIMD
// headers out of the common SerDe interface header.
struct SimdJsonValue {
    simdjson::ondemand::value& value;
};
// Reader-owned state, never stored in shared SerDes. Clear the cache before the next document:
// its keys and values borrow the input and parser buffers.
struct SimdJsonReadContext {
    bool use_string_cache = false;
    std::unordered_map<std::string_view, std::string_view> string_values;
    JsonbWriter jsonb_writer;

    JsonbParser::StringCache* cache() { return use_string_cache ? &string_values : nullptr; }
    std::string_view decoded_key(simdjson::ondemand::field& field) {
        return JsonbParser::decoded_key(field, cache());
    }
};
Status deserialize_simdjson_child(const DataTypeSerDe& serde, IColumn& column,
                                  simdjson::ondemand::value& value,
                                  const DataTypeSerDe::FormatOptions& options,
                                  SimdJsonReadContext& context, const std::string& column_name);
} // namespace doris
