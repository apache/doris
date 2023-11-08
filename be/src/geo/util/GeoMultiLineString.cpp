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

#include "GeoMultiLineString.h"

#include <sstream>

#include "GeoMultiPoint.h"

namespace doris {

GeoMultiLineString::GeoMultiLineString() = default;
GeoMultiLineString::~GeoMultiLineString() = default;

GeoParseStatus GeoMultiLineString::from_coords(const GeoCoordinateLists& coords_list) {
    for (int i = 0; i < coords_list.coords_list.size(); i++) {
        std::unique_ptr<doris::GeoLineString> line = GeoLineString::create_unique();
        if (line->from_coords(*coords_list.coords_list[i]) != GeoParseStatus::GEO_PARSE_OK) {
            return GEO_PARSE_COORD_INVALID;
        }
        geometries.emplace_back(line.release());
    }
    return GEO_PARSE_OK;
}

std::unique_ptr<GeoCoordinateLists> GeoMultiLineString::to_coords() const {
    std::unique_ptr<GeoCoordinateLists> coords_list(new GeoCoordinateLists());
    for (std::size_t i = 0; i < get_num_line(); ++i) {
        const GeoLineString* line = (const GeoLineString*)GeoCollection::get_geometries_n(i);
        if (line->is_empty()) continue;
        std::unique_ptr<GeoCoordinates> coords = line->to_coords_ptr();
        coords_list->add(coords.release());
    }
    return coords_list;
}

std::size_t GeoMultiLineString::get_num_line() const {
    return GeoCollection::get_num_geometries();
}

std::string GeoMultiLineString::as_wkt() const {
    std::stringstream ss;
    ss << "MULTILINESTRING ";

    if (is_empty()) {
        ss << "EMPTY";
        return ss.str();
    }

    ss << "(";
    for (int i = 0; i < get_num_line(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        const GeoLineString* line = (const GeoLineString*)GeoCollection::get_geometries_n(i);
        if (line->is_empty()) {
            ss << "EMPTY";
            continue;
        }

        ss << "(";

        for (std::size_t g = 0; g < line->num_point(); g++) {
            if (g != 0) {
                ss << ", ";
            }
            GeoPoint::print_s2point(ss, *line->get_point(g));
        }

        ss << ")";
    }
    ss << ")";
    return ss.str();
}

GeoLineString* GeoMultiLineString::get_line_string_n(std::size_t n) const {
    return (GeoLineString*)GeoCollection::get_geometries_n(n);
}

bool GeoMultiLineString::contains(const GeoShape* shape) const {
    return GeoCollection::contains(shape);
}

std::unique_ptr<GeoShape> GeoMultiLineString::boundary() const {
    std::unique_ptr<GeoMultiPoint> multipoint = GeoMultiPoint::create_unique();
    if (is_empty()) {
        multipoint->set_empty();
        return multipoint;
    }

    for (std::size_t i = 0; i < get_num_line(); ++i) {
        const GeoLineString* line = (const GeoLineString*)GeoCollection::get_geometries_n(i);
        if (line->is_empty() || line->is_ring()) {
            continue;
        }
        std::unique_ptr<GeoPoint> point1 = GeoPoint::create_unique();
        std::unique_ptr<GeoPoint> point2 = GeoPoint::create_unique();
        point1->from_s2point(line->get_point(0));
        point2->from_s2point(line->get_point(line->num_point() - 1));
        multipoint->add_one_geometry(point1.release());
        multipoint->add_one_geometry(point2.release());
    }

    if (multipoint->get_num_point() == 0) multipoint->set_empty();

    return multipoint;
}

} // namespace doris