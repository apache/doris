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

#include "vec/common/schema_util.h"

namespace doris {

class SchemaUtils {
public:
    static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                                 const std::string& column_type, const std::string& column_name,
                                 int variant_max_subcolumns_count = 3, bool is_key = false,
                                 bool is_nullable = false) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(is_key);
        column_pb->set_is_nullable(is_nullable);
        if (column_type == "VARIANT") {
            column_pb->set_variant_max_subcolumns_count(variant_max_subcolumns_count);
            column_pb->set_variant_max_sparse_column_statistics_size(10000);
        }
    }

    static void construct_tablet_index(TabletIndexPB* tablet_index, int64_t index_id,
                                       const std::string& index_name, int32_t col_unique_id) {
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
    }
};

} // namespace doris
