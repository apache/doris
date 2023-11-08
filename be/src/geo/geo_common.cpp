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

#include "geo/geo_common.h"

namespace doris {

std::string to_string(GeoParseStatus status) {
    switch (status) {
    case GEO_PARSE_OK:
        return "OK";
    case GEO_PARSE_COORD_INVALID:
        return "Coordinate invalid";
    case GEO_PARSE_LOOP_NOT_CLOSED:
        return "Loop is not closed";
    case GEO_PARSE_LOOP_LACK_VERTICES:
        return "Loop lack enough vertices";
    case GEO_PARSE_LOOP_INVALID:
        return "Loop invalid";
    case GEO_PARSE_POLYGON_NOT_HOLE:
        return "Loop not contained in the first loop";
    case GEO_PARSE_POLYLINE_LACK_VERTICES:
        return "Line string lack vertices";
    case GEO_PARSE_POLYLINE_INVALID:
        return "Line string invalid";
    case GEO_PARSE_CIRCLE_INVALID:
        return "Circle invalid";
    case GEO_PARSE_WKT_SYNTAX_ERROR:
        return "WKT syntax error";
    case GEO_PARSE_WKB_SYNTAX_ERROR:
        return "WKB syntax error";
    case GEO_PARSE_GEOJSON_SYNTAX_ERROR:
        return "GeoJson is invalid";
    default:
        return "Unknown";
    }
}

std::ostream& operator<<(std::ostream& os, GeoParseStatus status) {
    os << to_string(status);
    return os;
}

} // namespace doris
