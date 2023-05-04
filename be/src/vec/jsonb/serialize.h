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
#include <stddef.h>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"

namespace doris {
class TabletSchema;
class TupleDescriptor;

namespace vectorized {
class ColumnString;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
// use jsonb codec to store row format
class JsonbSerializeUtil {
public:
    static void block_to_jsonb(const TabletSchema& schema, const Block& block, ColumnString& dst,
                               int num_cols);
    // batch rows
    static void jsonb_to_block(const TupleDescriptor& desc, const ColumnString& jsonb_column,
                               Block& dst);
    // single row
    static void jsonb_to_block(const TupleDescriptor& desc, const char* data, size_t size,
                               Block& dst);

    static void jsonb_to_block(TabletSchemaSPtr schema, const std::vector<uint32_t>& col_ids,
                               const ColumnString& jsonb_column, Block& dst);

    static void jsonb_to_block(TabletSchemaSPtr schema, const std::vector<uint32_t>& col_ids,
                               const char* data, size_t size, Block& dst);

    static PrimitiveType get_primity_type(FieldType type);
};
} // namespace doris::vectorized