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

#include "geo_tojson.h"

#include "rapidjson/document.h"
#include "util/GeoLineString.h"
#include "util/GeoMultiLineString.h"
#include "util/GeoMultiPoint.h"
#include "util/GeoMultiPolygon.h"
#include "util/GeoPoint.h"
#include "util/GeoPolygon.h"
#include "util/GeoShape.h"

namespace doris {

bool toGeoJson::geo_tojson(GeoShape* shape, std::string& result) {
    switch (shape->type()) {
    case GEO_SHAPE_POINT: {
        return point_tojson((GeoPoint*)shape, result);
    }
    case GEO_SHAPE_LINE_STRING: {
        return linestring_tojson((GeoLineString*)shape, result);
    }
    case GEO_SHAPE_POLYGON: {
        return polygon_tojson((GeoPolygon*)shape, result);
    }
    case GEO_SHAPE_MULTI_POINT: {
        return multi_point_tojson((GeoMultiPoint*)shape, result);
    }
    case GEO_SHAPE_MULTI_LINE_STRING: {
        return multi_linestring_tojson((GeoMultiLineString*)shape, result);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        return multi_polygon_tojson((GeoMultiPolygon*)shape, result);
    }
    case GEO_SHAPE_GEOMETRY_COLLECTION: {
        return geo_collection_tojson((GeoCollection*)shape, result);
    }
    default:
        return false;
    }
}

bool toGeoJson::point_tojson(GeoPoint* point, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("Point", allocator);
    rapidjson::Value coord("coordinates", allocator);
    rapidjson::Value coord_array(rapidjson::kArrayType);
    if (!point->is_empty()) {
        GeoCoordinates coords = point->to_coords();
        coordinate_tojson(coords.coords[0], coord_array, allocator);
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, coord_array, allocator);
    return json_toString(&document, result);
}

bool toGeoJson::linestring_tojson(GeoLineString* linestring, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("LineString", allocator);
    rapidjson::Value coord("coordinates", allocator);
    rapidjson::Value coords_array(rapidjson::kArrayType);

    if (!linestring->is_empty()) {
        GeoCoordinates coords = linestring->to_coords();
        coordinates_tojson(coords, coords_array, allocator);
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, coords_array, allocator);
    return json_toString(&document, result);
}

bool toGeoJson::polygon_tojson(GeoPolygon* polygon, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("Polygon", allocator);
    rapidjson::Value coord("coordinates", allocator);
    rapidjson::Value coords_list_array(rapidjson::kArrayType);

    if (!polygon->is_empty()) {
        std::unique_ptr<GeoCoordinateLists> coords_list(polygon->to_coords());
        coordinates_list_tojson(*coords_list, coords_list_array, allocator);
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, coords_list_array, allocator);
    return json_toString(&document, result);
}

bool toGeoJson::multi_point_tojson(GeoMultiPoint* multipoint, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("MultiPoint", allocator);
    rapidjson::Value coord("coordinates", allocator);
    rapidjson::Value coords_array(rapidjson::kArrayType);

    if (!multipoint->is_empty()) {
        std::unique_ptr<GeoCoordinates> coords = multipoint->to_coords();
        coordinates_tojson(*coords, coords_array, allocator);
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, coords_array, allocator);
    return json_toString(&document, result);
}

bool toGeoJson::multi_linestring_tojson(GeoMultiLineString* multilinestring, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("MultiLineString", allocator);
    rapidjson::Value coord("coordinates", allocator);
    rapidjson::Value coords_list_array(rapidjson::kArrayType);

    if (!multilinestring->is_empty()) {
        std::unique_ptr<GeoCoordinateLists> coords_list = multilinestring->to_coords();
        coordinates_list_tojson(*coords_list, coords_list_array, allocator);
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, coords_list_array, allocator);
    return json_toString(&document, result);
}

bool toGeoJson::multi_polygon_tojson(GeoMultiPolygon* multipolygon, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("MultiPolygon", allocator);
    rapidjson::Value coord("coordinates", allocator);
    rapidjson::Value coords_list_collections_array(rapidjson::kArrayType);

    if (!multipolygon->is_empty()) {
        std::unique_ptr<GeoCoordinateListCollections> coords_collections =
                multipolygon->to_coords();
        coordinates_list_collections_tojson(*coords_collections, coords_list_collections_array,
                                            allocator);
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, coords_list_collections_array, allocator);
    return json_toString(&document, result);
}

bool toGeoJson::geo_collection_tojson(GeoCollection* collections, std::string& result) {
    std::shared_ptr<rapidjson::Document> document_ptr = std::make_shared<rapidjson::Document>();
    rapidjson::Document& document = *document_ptr;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value type("type", allocator);
    rapidjson::Value type_value("GeometryCollection", allocator);
    rapidjson::Value coord("geometries", allocator);
    rapidjson::Value geo_collections_array(rapidjson::kArrayType);

    if (!collections->is_empty()) {
        for (std::size_t i = 0; i < collections->get_num_geometries(); ++i) {
            GeoShape* shape = collections->get_geometries_n(i);
            std::string geo_json;
            if (geo_tojson(shape, geo_json)) {
                rapidjson::Document doc_tmp;
                doc_tmp.Parse(geo_json.c_str());
                if (doc_tmp.HasParseError()) {
                    return false;
                }
                rapidjson::Value a_value(rapidjson::kObjectType);
                a_value.CopyFrom(doc_tmp, allocator);
                geo_collections_array.PushBack(a_value, allocator);
            } else {
                return false;
            }
        }
    }
    document.AddMember(type, type_value, allocator);
    document.AddMember(coord, geo_collections_array, allocator);
    return json_toString(&document, result);
}

void toGeoJson::coordinates_list_collections_tojson(
        const GeoCoordinateListCollections& coords_list_collections,
        rapidjson::Value& coords_list_array, rapidjson::Document::AllocatorType& allocator) {
    for (std::size_t i = 0; i < coords_list_collections.coords_list_collections.size(); i++) {
        GeoCoordinateLists* coords_list = coords_list_collections.coords_list_collections[i];
        rapidjson::Value coords_array(rapidjson::kArrayType);
        coordinates_list_tojson(*coords_list, coords_array, allocator);
        coords_list_array.PushBack(coords_array, allocator);
    }
}

void toGeoJson::coordinates_list_tojson(const GeoCoordinateLists& coords_list,
                                        rapidjson::Value& coords_list_array,
                                        rapidjson::Document::AllocatorType& allocator) {
    for (std::size_t i = 0; i < coords_list.coords_list.size(); i++) {
        GeoCoordinates* coords = coords_list.coords_list[i];
        rapidjson::Value coords_array(rapidjson::kArrayType);
        coordinates_tojson(*coords, coords_array, allocator);
        coords_list_array.PushBack(coords_array, allocator);
    }
}

void toGeoJson::coordinates_tojson(const GeoCoordinates& coords, rapidjson::Value& coords_array,
                                   rapidjson::Document::AllocatorType& allocator) {
    for (std::size_t i = 0; i < coords.coords.size(); i++) {
        GeoCoordinate coord = coords.coords[i];
        rapidjson::Value coord_array(rapidjson::kArrayType);
        coordinate_tojson(coord, coord_array, allocator);
        coords_array.PushBack(coord_array, allocator);
    }
}

void toGeoJson::coordinate_tojson(const GeoCoordinate& coord, rapidjson::Value& coord_array,
                                  rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value coord_x(coord.x);
    coord_array.PushBack(coord_x, allocator);
    rapidjson::Value coord_y(coord.y);
    coord_array.PushBack(coord_y, allocator);
}

bool toGeoJson::json_toString(rapidjson::Document* document, std::string& result) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    document->Accept(writer);
    result = buf.GetString();
    return true;
}

} // namespace doris