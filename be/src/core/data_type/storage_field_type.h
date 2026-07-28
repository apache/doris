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

#include "core/data_type/define_primitive_type.h"

namespace doris {

enum class FieldType;

// Throws Exception when the primitive type has no storage field type mapping.
[[nodiscard]] FieldType primitive_type_to_storage_field_type(PrimitiveType type);

// Throws Exception when the storage field type has no primitive type mapping.
[[nodiscard]] PrimitiveType storage_field_type_to_primitive_type(FieldType type);

} // namespace doris
