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

#include "geo/geo_common.h"

namespace doris {

class GeoShape;
class GeoCollection;
class GeoMultiPoint;
class GeoMultiLineString;
class GeoMultiPolygon;

class WktParse {
public:
    // Parse WKT(Well Known Text) to a GeoShape.
    // Return a valid GeoShape if input WKT is supported.
    // Return null if WKT is not supported or invalid
    static GeoParseStatus parse_wkt(const char* str, size_t len, GeoShape** shape);
};

} // namespace doris
