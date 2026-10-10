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

#include <array>

#include "storage/field_type.h"

namespace doris {

// FILE's logical inline child is VARBINARY. Only its storage carrier is STRING;
// this does not introduce a storage mapping for ordinary VARBINARY columns.
inline constexpr std::array FILE_STORAGE_CHILD_TYPES = {
        FieldType::OLAP_FIELD_TYPE_VARCHAR, FieldType::OLAP_FIELD_TYPE_BIGINT,
        FieldType::OLAP_FIELD_TYPE_BIGINT,  FieldType::OLAP_FIELD_TYPE_VARCHAR,
        FieldType::OLAP_FIELD_TYPE_VARCHAR, FieldType::OLAP_FIELD_TYPE_STRING};
inline constexpr std::array FILE_STORAGE_CHILD_NAMES = {"uri",          "offset",   "size",
                                                        "content_type", "checksum", "inline"};

} // namespace doris
