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

#include "GeoMultiPoint.h"

#include <sstream>

namespace doris {

    GeoMultiPoint::GeoMultiPoint() = default;
    GeoMultiPoint::~GeoMultiPoint() = default;

    GeoParseStatus GeoMultiPoint::from_coords(const GeoCoordinates& coord_list) {
        for(int i = 0; i < coord_list.coords.size(); i++){
            std::unique_ptr<doris::GeoPoint> point = GeoPoint::create_unique();
            if(point->from_coord(coord_list.coords[i]) != GeoParseStatus::GEO_PARSE_OK){
                return GEO_PARSE_COORD_INVALID;
            }
            geometries.emplace_back(point.release());
        }
        return GEO_PARSE_OK;
    }

    std::unique_ptr<GeoCoordinates> GeoMultiPoint::to_coords() const {

        std::unique_ptr<GeoCoordinates> coords(new GeoCoordinates());
        for (std::size_t i = 0; i < get_num_point(); ++i) {
            const GeoPoint* point = (const GeoPoint*) GeoCollection::get_geometries_n(i);
            std::unique_ptr<GeoCoordinate> coord = point->to_coord();
            coords->add(*coord);
        }
        return coords;
    }

    std::string GeoMultiPoint::as_wkt() const {
        std::stringstream ss;
        ss << "MULTIPOINT ";

        if (is_empty()){
            ss << "EMPTY";
            return ss.str();
        }
        ss << "(";

        for (int i = 0; i < get_num_point(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            const GeoPoint* point = (const GeoPoint*) GeoCollection::get_geometries_n(i);
            if(point->is_empty()){
                ss << "EMPTY";
            } else {
                GeoPoint::print_s2point(ss,*point->point());
            }
        }
        ss << ")";
        return ss.str();
    }

    GeoPoint* GeoMultiPoint::get_point_n(std::size_t n) const {
        return (GeoPoint*)GeoCollection::get_geometries_n(n);
    }

    bool GeoMultiPoint::contains(const GeoShape* shape) const {
        return GeoCollection::contains(shape);
    }

    std::unique_ptr<GeoShape> GeoMultiPoint::boundary() const {
        std::unique_ptr<GeoMultiPoint> multipoint = GeoMultiPoint::create_unique();
        multipoint->set_empty();
        return multipoint;
    }

}// namespace namespace doris
