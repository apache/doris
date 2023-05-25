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

#include "GeoMultiPolygon.h"
#include "GeoMultiLineString.h"
#include <sstream>

namespace doris {

    GeoMultiPolygon::GeoMultiPolygon() = default;
    GeoMultiPolygon::~GeoMultiPolygon() = default;

    GeoParseStatus GeoMultiPolygon::from_coords(const GeoCoordinateListCollections& coord_list_collections){
        for(int i = 0; i < coord_list_collections.coords_list_collections.size(); i++){
            std::unique_ptr<doris::GeoPolygon> polygon = GeoPolygon::create_unique();
            if(polygon->from_coords(*coord_list_collections.coords_list_collections[i]) != GeoParseStatus::GEO_PARSE_OK){
                return GEO_PARSE_COORD_INVALID;
            }
            geometries.emplace_back(polygon.release());
        }
        return GEO_PARSE_OK;
    }

    std::unique_ptr<GeoCoordinateListCollections> GeoMultiPolygon::to_coords() const{
        std::unique_ptr<GeoCoordinateListCollections> coords_collections(new GeoCoordinateListCollections());
        for (std::size_t i = 0; i < get_num_polygon(); ++i) {
            const GeoPolygon* polygon = (const GeoPolygon*) GeoCollection::get_geometries_n(i);
            std::unique_ptr<GeoCoordinateLists> coord = polygon->to_coords();
            coords_collections->add(coord.release());
        }
        return coords_collections;
    }


    std::size_t GeoMultiPolygon::get_num_polygon() const {
        return GeoCollection::get_num_geometries();
    }

    std::string GeoMultiPolygon::as_wkt() const {
        std::stringstream ss;
        ss << "MULTIPOLYGON ";
        if (is_empty()){
            ss << "EMPTY";
            return ss.str();
        }

        ss << "(";

        for (int i = 0; i < get_num_polygon(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            const GeoPolygon* polygon = (const GeoPolygon*) GeoCollection::get_geometries_n(i);
            if(polygon->is_empty()){
                ss << "EMPTY";
            } else {
                GeoPolygon::print_coords(ss,*polygon);
            }
        }
        ss << ")";
        return ss.str();
    }

    GeoPolygon* GeoMultiPolygon::get_polygon_n(std::size_t n) const {
        return (GeoPolygon*)GeoCollection::get_geometries_n(n);
    }

    bool GeoMultiPolygon::contains(const GeoShape* shape) const {
        return GeoCollection::contains(shape);
    }

    std::unique_ptr<GeoShape> GeoMultiPolygon::boundary() const {
        std::unique_ptr<GeoMultiLineString> multi_linestring = GeoMultiLineString::create_unique();
        if(is_empty()){
            multi_linestring->set_empty();
            return multi_linestring;
        }

        std::unique_ptr<GeoCoordinateListCollections> coords_list_collections =  to_coords();

        if(coords_list_collections->coords_list_collections.size() == 1 && coords_list_collections->coords_list_collections[0]->coords_list.size() == 1){
            std::unique_ptr<GeoLineString> linestring = GeoLineString::create_unique();
            linestring->from_coords(*coords_list_collections->coords_list_collections[0]->coords_list[0]);
            return linestring;
        }

        for (int i = 0; i < coords_list_collections->coords_list_collections.size(); ++i) {
            for (int j = 0; j < coords_list_collections->coords_list_collections[i]->coords_list.size(); ++j) {
                std::unique_ptr<GeoLineString> linestring = GeoLineString::create_unique();
                linestring->from_coords(*coords_list_collections->coords_list_collections[i]->coords_list[j]);
                multi_linestring->add_one_geometry(linestring.release());
            }
        }

        return multi_linestring;
    }

} // namespace doris