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

#include "core/data_type/primitive_type.h"
#include "core/data_type/storage_field_type.h"
#include "core/field.h"
#include "storage/storage_layout.h"

namespace doris {

// The compute-layer Field a StorageValue of PT's FieldType decodes to.
template <PrimitiveType PT>
Field field_of_cell(
        const typename StorageLayout<primitive_type_to_storage_field_type(PT)>::StorageValue&
                value) {
    return Field::create_field<PT>(
            StorageLayout<primitive_type_to_storage_field_type(PT)>::to_primitive(value));
}

} // namespace doris
