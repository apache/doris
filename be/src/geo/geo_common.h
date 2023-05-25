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

#include <ostream>
#include <string>

namespace doris {

enum GeoShapeType {
    GEO_SHAPE_ANY = 0,
    GEO_SHAPE_POINT = 1,
    GEO_SHAPE_LINE_STRING = 2,
    GEO_SHAPE_POLYGON = 3,
    GEO_SHAPE_MULTI_POINT = 4,
    GEO_SHAPE_MULTI_LINE_STRING = 5,
    GEO_SHAPE_MULTI_POLYGON = 6,
    GEO_SHAPE_GEOMETRY_COLLECTION = 7,
    GEO_SHAPE_CIRCLE = 8,
};

enum GeoParseStatus {
    GEO_PARSE_OK = 0,
    GEO_PARSE_COORD_INVALID = 1,
    GEO_PARSE_LOOP_NOT_CLOSED = 2,
    GEO_PARSE_LOOP_LACK_VERTICES = 3,
    GEO_PARSE_LOOP_INVALID = 4,
    GEO_PARSE_POLYGON_NOT_HOLE = 5,
    GEO_PARSE_POLYLINE_LACK_VERTICES = 6,
    GEO_PARSE_POLYLINE_INVALID = 7,
    GEO_PARSE_CIRCLE_INVALID = 8,
    GEO_PARSE_WKT_SYNTAX_ERROR = 9,
    GEO_PARSE_WKB_SYNTAX_ERROR = 10,
    GEO_PARSE_GEOJSON_SYNTAX_ERROR = 11,
};

std::string to_string(GeoParseStatus status);
std::ostream& operator<<(std::ostream& os, GeoParseStatus status);

} // namespace doris
