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

#include "format/parquet/parquet_zone_map_adapter.h"

namespace doris {

bool ParquetZoneMapAdapter::from_column_stat(const ParquetPredicate::ColumnStat& col_stat,
                                             segment_v2::ZoneMap* zone_map) {
    if (zone_map == nullptr || col_stat.col_schema == nullptr) {
        return false;
    }
    zone_map->has_null = col_stat.has_null;
    zone_map->has_not_null = !col_stat.is_all_null;
    zone_map->pass_all = false;
    if (col_stat.is_all_null) {
        return true;
    }

    Field min_value;
    Field max_value;
    if (!ParquetPredicate::parse_min_max_value(col_stat.col_schema, col_stat.encoded_min_value,
                                               col_stat.encoded_max_value, *col_stat.ctz,
                                               &min_value, &max_value)
                 .ok()) {
        zone_map->pass_all = true;
        return false;
    }
    zone_map->min_value = std::move(min_value);
    zone_map->max_value = std::move(max_value);
    return true;
}

} // namespace doris
