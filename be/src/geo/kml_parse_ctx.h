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

#include "geo/geo_common.h"
#include "geo/geo_types.h"
#include "geo/wkt_parse_type.h"

namespace doris {

// Parsing context state for KML geometry
struct KmlParseContext {
    // ========================== Global State ==========================
    GeoShapeType current_shape_type =
            GEO_SHAPE_ANY;       // Type of geometry being parsed (Point/LineString/Polygon)
    bool in_coordinates = false; // Flag for <coordinates> tag scope
    std::string char_buffer;     // Temporary buffer for character data accumulation

    // ========================== Geometry Data Storage ==========================
    // GeoCoordinateList for Point/LineString
    GeoCoordinateList* coordinates = nullptr;

    // Polygon-specific parsing state
    struct {
        bool in_outer_boundary = false; // Inside <outerBoundaryIs> tag
        bool in_inner_boundary = false; // Inside <innerBoundaryIs> tag
        GeoCoordinateListList rings;    // Coordinate rings (outer + inner)
    } polygon_ctx;

    // ========================== Output Handling ==========================
    std::unique_ptr<GeoShape> shape = nullptr;
    GeoParseStatus status = GEO_PARSE_OK; // Parsing status code

    // ========================== Utility Methods ==========================
    // Reset temporary state before parsing new geometry
    void reset() {
        current_shape_type = GEO_SHAPE_ANY;
        in_coordinates = false;
        char_buffer.clear();
        if (coordinates == nullptr) {
            coordinates = new GeoCoordinateList();
        }
        coordinates->clear();
        polygon_ctx = {}; // Reset polygon context
        shape = nullptr;
        status = GEO_PARSE_OK;
    }

    ~KmlParseContext() { delete coordinates; }

    // Parse coordinate string (format: "lng,lat lng,lat...]")
    bool parse_coordinates(const std::string& str) {
        std::istringstream iss(str);
        std::string token;
        while (std::getline(iss, token, ' ')) {
            size_t comma_pos = token.find(',');
            if (comma_pos == std::string::npos) {
                status = GEO_PARSE_COORD_INVALID;
                return false;
            }
            try {
                double lng = std::stod(token.substr(0, comma_pos));
                double lat = std::stod(token.substr(comma_pos + 1));
                coordinates->add(lng, lat);
            } catch (...) {
                status = GEO_PARSE_COORD_INVALID;
                return false;
            }
        }
        return true;
    }

    // make parsed geometry into GeoShape
    void build_shape() {
        switch (current_shape_type) {
        case GEO_SHAPE_POINT: {
            if (coordinates->list.size() == 1) {
                auto point = GeoPoint::create_unique();
                status = point->from_coord(coordinates->list[0]);
                if (status == GEO_PARSE_OK) {
                    shape = std::move(point);
                }
            } else {
                status = GEO_PARSE_POINT_INVALID;
            }
            break;
        }
        case GEO_SHAPE_LINE_STRING: {
            auto line = GeoLine::create_unique();
            status = line->from_coords(*coordinates);
            if (status == GEO_PARSE_OK) {
                shape = std::move(line);
            }
            break;
        }
        case GEO_SHAPE_POLYGON: {
            auto polygon = GeoPolygon::create_unique();
            status = polygon->from_coords(polygon_ctx.rings);
            if (status == GEO_PARSE_OK) {
                shape = std::move(polygon);
            }
            break;
        }
        default:
            status = GEO_PARSE_XML_SYNTAX_ERROR;
            break;
        }
    }
};

} // namespace doris
