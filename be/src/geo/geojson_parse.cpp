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

#include "geojson_parse.h"

#include <stdint.h>

#include <memory>

#include "rapidjson/document.h"
#include "util/GeoCollection.h"
#include "util/GeoLineString.h"
#include "util/GeoMultiLineString.h"
#include "util/GeoMultiPoint.h"
#include "util/GeoMultiPolygon.h"
#include "util/GeoPoint.h"
#include "util/GeoPolygon.h"
#include "util/GeoShape.h"

namespace doris {
GeoParseStatus GeoJsonParse::parse_geojson(std::string& geojson, GeoShape** shape) {
    rapidjson::Document document;
    document.Parse(geojson.c_str());

    // 判断解析是否成功
    if (document.HasParseError()) {
        return GEO_PARSE_GEOJSON_SYNTAX_ERROR;
    }
    GeoParseStatus parse_status;

    *shape = read_geometry(document, parse_status).release();

    return parse_status;
}

std::unique_ptr<GeoShape> GeoJsonParse::read_geometry(rapidjson::Document& document,
                                                      GeoParseStatus& parse_status) {
    std::unique_ptr<GeoShape> shape;
    if (document.HasMember("type") && document["type"].IsString()) {
        std::string type = document["type"].GetString();
        if (type == "Point") {
            shape.reset(read_point(document, parse_status).release());
        } else if (type == "LineString") {
            shape.reset(read_linestring(document, parse_status).release());
        } else if (type == "Polygon") {
            shape.reset(read_polygon(document, parse_status).release());
        } else if (type == "MultiPoint") {
            shape.reset(read_multi_point(document, parse_status).release());
        } else if (type == "MultiLineString") {
            shape.reset(read_multi_linestring(document, parse_status).release());
        } else if (type == "MultiPolygon") {
            shape.reset(read_multi_polygon(document, parse_status).release());
        } else if (type == "GeometryCollection") {
            shape.reset(read_geometry_collection(document, parse_status).release());
        } else {
            return nullptr;
        }
    }
    return shape;
}

std::unique_ptr<GeoPoint> GeoJsonParse::read_point(rapidjson::Document& document,
                                                   GeoParseStatus& parse_status) {
    std::unique_ptr<GeoPoint> point = GeoPoint::create_unique();
    if (document.HasMember("coordinates") && document["coordinates"].IsArray()) {
        if (document["coordinates"].Size() == 0) {
            point->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            std::unique_ptr<GeoCoordinate> coord =
                    read_coordinate(document["coordinates"], parse_status);
            if (parse_status == GEO_PARSE_OK) {
                parse_status = point->from_coord(*coord);
            }
        }
    }
    return point;
}

std::unique_ptr<GeoLineString> GeoJsonParse::read_linestring(rapidjson::Document& document,
                                                             GeoParseStatus& parse_status) {
    std::unique_ptr<GeoLineString> linestring = GeoLineString::create_unique();
    if (document.HasMember("coordinates") && document["coordinates"].IsArray()) {
        if (document["coordinates"].Size() == 0) {
            linestring->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            std::unique_ptr<GeoCoordinates> coords =
                    read_coordinates(document["coordinates"], parse_status);
            if (parse_status == GEO_PARSE_OK) {
                parse_status = linestring->from_coords(*coords);
            }
        }
    }
    return linestring;
}

std::unique_ptr<GeoPolygon> GeoJsonParse::read_polygon(rapidjson::Document& document,
                                                       GeoParseStatus& parse_status) {
    std::unique_ptr<GeoPolygon> polygon = GeoPolygon::create_unique();
    if (document.HasMember("coordinates") && document["coordinates"].IsArray()) {
        if (document["coordinates"].Size() == 0) {
            polygon->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            std::unique_ptr<GeoCoordinateLists> coords_list =
                    read_coords_list(document["coordinates"], parse_status);
            if (parse_status == GEO_PARSE_OK) {
                parse_status = polygon->from_coords(*coords_list);
            }
        }
    }
    return polygon;
}

std::unique_ptr<GeoMultiPoint> GeoJsonParse::read_multi_point(rapidjson::Document& document,
                                                              GeoParseStatus& parse_status) {
    std::unique_ptr<GeoMultiPoint> multi_point = GeoMultiPoint::create_unique();
    if (document.HasMember("coordinates") && document["coordinates"].IsArray()) {
        if (document["coordinates"].Size() == 0) {
            multi_point->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            std::unique_ptr<GeoCoordinates> coords =
                    read_coordinates(document["coordinates"], parse_status);
            if (parse_status == GEO_PARSE_OK) {
                parse_status = multi_point->from_coords(*coords);
            }
        }
    }
    return multi_point;
}

std::unique_ptr<GeoMultiLineString> GeoJsonParse::read_multi_linestring(
        rapidjson::Document& document, GeoParseStatus& parse_status) {
    std::unique_ptr<GeoMultiLineString> multi_linestring = GeoMultiLineString::create_unique();
    if (document.HasMember("coordinates") && document["coordinates"].IsArray()) {
        if (document["coordinates"].Size() == 0) {
            multi_linestring->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            std::unique_ptr<GeoCoordinateLists> coords_list =
                    read_coords_list(document["coordinates"], parse_status);
            if (parse_status == GEO_PARSE_OK) {
                parse_status = multi_linestring->from_coords(*coords_list);
            }
        }
    }
    return multi_linestring;
}

std::unique_ptr<GeoMultiPolygon> GeoJsonParse::read_multi_polygon(rapidjson::Document& document,
                                                                  GeoParseStatus& parse_status) {
    std::unique_ptr<GeoMultiPolygon> multi_polygon = GeoMultiPolygon::create_unique();
    if (document.HasMember("coordinates") && document["coordinates"].IsArray()) {
        if (document["coordinates"].Size() == 0) {
            multi_polygon->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            std::unique_ptr<GeoCoordinateListCollections> coords_list_collections =
                    read_coords_list_collections(document["coordinates"], parse_status);
            if (parse_status == GEO_PARSE_OK) {
                parse_status = multi_polygon->from_coords(*coords_list_collections);
            }
        }
    }
    return multi_polygon;
}

std::unique_ptr<GeoCollection> GeoJsonParse::read_geometry_collection(
        rapidjson::Document& document, GeoParseStatus& parse_status) {
    std::unique_ptr<GeoCollection> collection = GeoCollection::create_unique();
    if (document.HasMember("geometries") && document["geometries"].IsArray()) {
        if (document["geometries"].Size() == 0) {
            collection->set_empty();
            parse_status = GEO_PARSE_OK;
        } else {
            for (std::size_t i = 0; i < document["geometries"].Size(); ++i) {
                rapidjson::Document doc;
                doc.CopyFrom(document["geometries"][i], doc.GetAllocator());
                auto status =
                        collection->add_one_geometry(read_geometry(doc, parse_status).release());
                if (parse_status != GEO_PARSE_OK || status != GEO_PARSE_OK) {
                    return nullptr;
                }
            }
        }
    }
    return collection;
}

std::unique_ptr<GeoCoordinateListCollections> GeoJsonParse::read_coords_list_collections(
        rapidjson::Value& coords_array, GeoParseStatus& parse_status) {
    std::unique_ptr<GeoCoordinateListCollections> coords_list_collections(
            new GeoCoordinateListCollections());
    for (int i = 0; i < coords_array.Size(); ++i) {
        std::unique_ptr<GeoCoordinateLists> coords_list =
                read_coords_list(coords_array[i], parse_status);
        if (parse_status != GEO_PARSE_OK) {
            return nullptr;
        }
        coords_list_collections->add(coords_list.release());
    }
    return coords_list_collections;
}

std::unique_ptr<GeoCoordinateLists> GeoJsonParse::read_coords_list(rapidjson::Value& coords_array,
                                                                   GeoParseStatus& parse_status) {
    std::unique_ptr<GeoCoordinateLists> coords_list(new GeoCoordinateLists());
    for (int i = 0; i < coords_array.Size(); ++i) {
        std::unique_ptr<GeoCoordinates> coords = read_coordinates(coords_array[i], parse_status);
        if (parse_status != GEO_PARSE_OK) {
            return nullptr;
        }
        coords_list->add(coords.release());
    }
    return coords_list;
}

std::unique_ptr<GeoCoordinates> GeoJsonParse::read_coordinates(rapidjson::Value& coords_array,
                                                               GeoParseStatus& parse_status) {
    std::unique_ptr<GeoCoordinates> coords(new GeoCoordinates());
    for (int i = 0; i < coords_array.Size(); ++i) {
        std::unique_ptr<GeoCoordinate> coord = read_coordinate(coords_array[i], parse_status);
        if (parse_status != GEO_PARSE_OK) {
            return nullptr;
        }
        coords->add(*coord);
    }
    return coords;
}

std::unique_ptr<GeoCoordinate> GeoJsonParse::read_coordinate(rapidjson::Value& coord_array,
                                                             GeoParseStatus& parse_status) {
    if (LIKELY(coord_array.Size() == 2 && (coord_array[0].IsDouble() || coord_array[0].IsInt()) &&
               (coord_array[1].IsDouble() || coord_array[1].IsInt()))) {
        parse_status = GEO_PARSE_OK;
        return std::make_unique<GeoCoordinate>(coord_array[0].GetDouble(),
                                               coord_array[1].GetDouble());
    } else {
        parse_status = GEO_PARSE_GEOJSON_SYNTAX_ERROR;
        return nullptr;
    }
}
} // namespace doris