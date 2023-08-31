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

#include <memory>

#include "geo/geo_common.h"
#include "geo/wkt_parse_type.h"
#include "rapidjson/document.h"

namespace doris {
class GeoShape;
class GeoPoint;
class GeoLineString;
class GeoPolygon;
class GeoMultiPoint;
class GeoMultiLineString;
class GeoMultiPolygon;
class GeoCollection;

class GeoJsonParse {
public:
    static GeoParseStatus parse_geojson(std::string& geojson, GeoShape** shape);

    static std::unique_ptr<GeoShape> read_geometry(rapidjson::Document& document,
                                                   GeoParseStatus& status);

private:
    static std::unique_ptr<GeoPoint> read_point(rapidjson::Document& document,
                                                GeoParseStatus& status);

    static std::unique_ptr<GeoLineString> read_linestring(rapidjson::Document& document,
                                                          GeoParseStatus& status);

    static std::unique_ptr<GeoPolygon> read_polygon(rapidjson::Document& document,
                                                    GeoParseStatus& status);

    static std::unique_ptr<GeoMultiPoint> read_multi_point(rapidjson::Document& document,
                                                           GeoParseStatus& status);

    static std::unique_ptr<GeoMultiLineString> read_multi_linestring(rapidjson::Document& document,
                                                                     GeoParseStatus& status);

    static std::unique_ptr<GeoMultiPolygon> read_multi_polygon(rapidjson::Document& document,
                                                               GeoParseStatus& status);

    static std::unique_ptr<GeoCollection> read_geometry_collection(rapidjson::Document& document,
                                                                   GeoParseStatus& status);

    static std::unique_ptr<GeoCoordinateListCollections> read_coords_list_collections(
            rapidjson::Value& coords_array, GeoParseStatus& status);

    static std::unique_ptr<GeoCoordinateLists> read_coords_list(rapidjson::Value& coords_array,
                                                                GeoParseStatus& status);

    static std::unique_ptr<GeoCoordinates> read_coordinates(rapidjson::Value& coords_array,
                                                            GeoParseStatus& status);

    static std::unique_ptr<GeoCoordinate> read_coordinate(rapidjson::Value& coord_array,
                                                          GeoParseStatus& status);
};

} // namespace doris
