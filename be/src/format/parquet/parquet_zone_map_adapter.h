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

#include "format/parquet/parquet_predicate.h"
#include "storage/index/zone_map/zone_map_index.h"

namespace doris {

class ParquetZoneMapAdapter {
public:
    static bool from_column_stat(const ParquetPredicate::ColumnStat& col_stat,
                                 segment_v2::ZoneMap* zone_map);
};

} // namespace doris
