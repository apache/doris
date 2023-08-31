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

#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <string>

namespace doris {

class GeoShape;
class GeoPoint;
class GeoLineString;
class GeoPolygon;
class GeoCollection;
class GeoMultiPoint;
class GeoMultiLineString;
class GeoMultiPolygon;
class GeoCollection;

struct GeoCoordinate;
struct GeoCoordinates;
struct GeoCoordinateLists;
struct GeoCoordinateListCollections;

class toGeoJson {
public:
    static bool geo_tojson(GeoShape* shape, std::string& result);

private:
    static bool point_tojson(GeoPoint* point, std::string& result);

    static bool linestring_tojson(GeoLineString* linestring, std::string& result);

    static bool polygon_tojson(GeoPolygon* polygon, std::string& result);

    static bool multi_point_tojson(GeoMultiPoint* multipoint, std::string& result);

    static bool multi_linestring_tojson(GeoMultiLineString* multilinestring, std::string& result);

    static bool multi_polygon_tojson(GeoMultiPolygon* multipolygon, std::string& result);

    static bool geo_collection_tojson(GeoCollection* collections, std::string& result);

    static void coordinates_list_collections_tojson(
            const GeoCoordinateListCollections& coords_list_collections,
            rapidjson::Value& coords_list_array, rapidjson::Document::AllocatorType& allocator);

    static void coordinates_list_tojson(const GeoCoordinateLists& coords_list,
                                        rapidjson::Value& coords_list_array,
                                        rapidjson::Document::AllocatorType& allocator);

    static void coordinates_tojson(const GeoCoordinates& coords, rapidjson::Value& coords_array,
                                   rapidjson::Document::AllocatorType& allocator);

    static void coordinate_tojson(const GeoCoordinate& coord, rapidjson::Value& coord_array,
                                  rapidjson::Document::AllocatorType& allocator);

    static bool json_toString(rapidjson::Document* document, std::string& result);
};

} // namespace doris
