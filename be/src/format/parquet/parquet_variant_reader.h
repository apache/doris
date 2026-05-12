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

#include <cstdint>
#include <deque>
#include <string>

#include "common/status.h"
#include "core/field.h"
#include "core/string_ref.h"

namespace doris::parquet {

std::string format_variant_uuid(const uint8_t* ptr);

Status decode_variant_to_json(const StringRef& metadata, const StringRef& value, std::string* json);

Status decode_variant_to_variant_map(const StringRef& metadata, const StringRef& value,
                                     const PathInData& prefix, VariantMap* values,
                                     std::deque<std::string>* string_values);

} // namespace doris::parquet
