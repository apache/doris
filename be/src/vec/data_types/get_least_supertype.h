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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/getLeastSupertype.h
// and modified by Doris

#pragma once

#include <parallel_hashmap/phmap.h>

#include "common/status.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
enum class TypeIndex;
} // namespace doris::vectorized

namespace doris::vectorized {

using TypeIndexSet = phmap::flat_hash_set<TypeIndex>;

void get_least_supertype_jsonb(const DataTypes& types, DataTypePtr* type);
void get_least_supertype_jsonb(const TypeIndexSet& types, DataTypePtr* type);

} // namespace doris::vectorized
