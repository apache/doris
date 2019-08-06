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

#include <string>
#include "olap/tablet_schema.h"

namespace doris {

TabletColumn create_int_key(int32_t id, bool is_nullable = true) {
    TabletColumn column;
    column._unique_id = id;
    column._col_name = std::to_string(id);
    column._type = OLAP_FIELD_TYPE_INT;
    column._is_key = true;
    column._is_nullable = is_nullable;
    column._length = 4;
    column._index_length = 4;
    return column;
}


TabletColumn create_int_value(
        int32_t id,
        FieldAggregationMethod agg_method = OLAP_FIELD_AGGREGATION_SUM,
        bool is_nullable = true) {
    TabletColumn column;
    column._unique_id = id;
    column._col_name = std::to_string(id);
    column._type = OLAP_FIELD_TYPE_INT;
    column._is_key = false;
    column._aggregation = agg_method;
    column._is_nullable = is_nullable;
    column._length = 4;
    column._index_length = 4;
    return column;
}

}
