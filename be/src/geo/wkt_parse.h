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

#include <cstddef>
#include <memory>

#include "geo/geo_common.h"
#include "geo/wkt_parse_ctx.h"

namespace doris {

class GeoShape;

class WktParse {
public:
    // Parse WKT(Well Known Text) to a GeoShape.
    // Return a valid GeoShape if input WKT is supported.
    // Return null if WKT is not supported or invalid
    static GeoParseStatus parse_wkt(const char* str, size_t len, std::unique_ptr<GeoShape>& shape);

private:
    static int wkt_parse(const char* str, size_t len, WktParseContext& ctx);
};

} // namespace doris
