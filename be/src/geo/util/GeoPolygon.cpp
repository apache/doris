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

#include "GeoPolygon.h"
#include <s2/s2polygon.h>
#include <s2/mutable_s2shape_index.h>
#include <s2/util/coding/coder.h>
#include <vector>
#include "GeoMultiLineString.h"

namespace doris {

    GeoPolygon::GeoPolygon() : _polygon(new S2Polygon()) {}
    GeoPolygon::~GeoPolygon() = default;

    static bool is_loop_closed(const std::vector<S2Point>& points) {
        if (points.empty()) {
            return false;
        }
        if (points[0] != points[points.size() - 1]) {
            return false;
        }
        return true;
    }

    // remove adjacent duplicate points
    static void remove_duplicate_points(std::vector<S2Point>* points) {
        int lhs = 0;
        int rhs = 1;
        for (; rhs < points->size(); ++rhs) {
            if ((*points)[rhs] != (*points)[lhs]) {
                lhs++;
                if (lhs != rhs) {
                    (*points)[lhs] = (*points)[rhs];
                }
            }
        }
        points->resize(lhs + 1);
    }

     GeoParseStatus to_s2loop(const GeoCoordinates& coords, std::unique_ptr<S2Loop>* loop) {
        // 1. convert all coordinates to points
        std::vector<S2Point> points(coords.coords.size());
        for (int i = 0; i < coords.coords.size(); ++i) {
            auto res = GeoPoint::to_s2point(coords.coords[i].x,coords.coords[i].y, &points[i]);
            if (res != GEO_PARSE_OK) {
                return res;
            }
        }
        // 2. check if it is a closed loop
        if (!is_loop_closed(points)) {
            return GEO_PARSE_LOOP_NOT_CLOSED;
        }
        // 3. remove duplicate points
        remove_duplicate_points(&points);
        // 4. remove last point
        points.resize(points.size() - 1);
        // 5. check if there is enough point
        if (points.size() < 3) {
            return GEO_PARSE_LOOP_LACK_VERTICES;
        }
        loop->reset(new S2Loop(points));
        if (!(*loop)->IsValid()) {
            return GEO_PARSE_LOOP_INVALID;
        }
        (*loop)->Normalize();
        return GEO_PARSE_OK;
    }

    static GeoParseStatus to_s2polygon(const GeoCoordinateLists& coords_list,
                                       std::unique_ptr<S2Polygon>* polygon) {
        std::vector<std::unique_ptr<S2Loop>> loops(coords_list.coords_list.size());
        for (int i = 0; i < coords_list.coords_list.size(); ++i) {
            auto res = to_s2loop(*coords_list.coords_list[i], &loops[i]);
            if (res != GEO_PARSE_OK) {
                return res;
            }
            if (i != 0 && !(loops[0]->Contains(*loops[i]))) {
                return GEO_PARSE_POLYGON_NOT_HOLE;
            }
        }
        polygon->reset(new S2Polygon(std::move(loops)));
        return GEO_PARSE_OK;
    }

    bool GeoPolygon::is_valid() const {
        return polygon()->IsValid();
    }

    bool GeoPolygon::is_closed() const {
        return polygon()->is_full();
    }

    GeoParseStatus GeoPolygon::from_coords(const GeoCoordinateLists& list) {
        return to_s2polygon(list, &_polygon);
    }

    const std::unique_ptr<GeoCoordinateLists> GeoPolygon::to_coords() const {
        std::unique_ptr<GeoCoordinateLists> coordss(new GeoCoordinateLists());
        for (int i = 0; i < GeoPolygon::num_loops(); ++i) {
            std::unique_ptr<GeoCoordinates> coords(new GeoCoordinates());
            S2Loop* loop = GeoPolygon::get_loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                GeoCoordinate coord;
                coord.x = std::stod(
                        absl::StrFormat("%.13f", S2LatLng::Longitude(loop->vertex(j)).degrees()));
                coord.y = std::stod(
                        absl::StrFormat("%.13f", S2LatLng::Latitude(loop->vertex(j)).degrees()));
                coords->add(coord);
                if (j == loop->num_vertices() - 1) {
                    coord.x = std::stod(
                            absl::StrFormat("%.13f", S2LatLng::Longitude(loop->vertex(0)).degrees()));
                    coord.y = std::stod(
                            absl::StrFormat("%.13f", S2LatLng::Latitude(loop->vertex(0)).degrees()));
                    coords->add(coord);
                }
            }
            coordss->add(coords.release());
        }
        return coordss;
    }

    GeoParseStatus GeoPolygon::from_s2loop(std::vector<std::unique_ptr<S2Loop>>& loops){
        _polygon.reset(new S2Polygon(std::move(loops)));
        return GEO_PARSE_OK;
    }

    GeoParseStatus GeoPolygon::from_s2polygon(std::unique_ptr<S2Polygon> s2polygon){
        _polygon.reset(s2polygon.release());
        return GEO_PARSE_OK;
    }

    double GeoPolygon::get_perimeter() const{
        double perimeter = 0;
        std::unique_ptr<S2Shape> shape = get_s2shape();
        for (int i = 0; i < shape->num_edges(); i++) {
            S2Shape::Edge e = shape->edge(i);
            S1ChordAngle angle(e.v0, e.v1);
            perimeter += angle.radians();
        }
        return perimeter;
    }

    std::unique_ptr<S2Shape> GeoPolygon::get_s2shape() const{
        return std::make_unique<S2Polygon::Shape> (_polygon.get());
    }

    bool GeoPolygon::contains(const GeoShape* rhs) const {
        switch (rhs->type()) {
            case GEO_SHAPE_POINT: {
                const GeoPoint* point = (const GeoPoint*)rhs;
                return _polygon->Contains(*point->point());
            }
            case GEO_SHAPE_LINE_STRING: {
                const GeoLineString* line = (const GeoLineString*)rhs;
                return _polygon->Contains(*line->polyline());
            }
            case GEO_SHAPE_POLYGON: {
                const GeoPolygon* polygon = (const GeoPolygon*)rhs;
                return _polygon->Contains(*polygon->polygon());
            }
            default:
                return false;
        }
    }

    void GeoPolygon::print_coords(std::ostream& os, const GeoPolygon& polygon) {
        os << "(";
        for (int i = 0; i < polygon.num_loops(); ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << "(";
            const S2Loop* loop = polygon.get_loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                if (j != 0) {
                    os << ", ";
                }
                GeoPoint::print_s2point(os, loop->vertex(j));
            }
            os << ", ";
            GeoPoint::print_s2point(os, loop->vertex(0));
            os << ")";
        }
        os << ")";

    }

    std::string GeoPolygon::as_wkt() const {
        std::stringstream ss;
        ss << "POLYGON ";

        if (is_empty()){
            ss << "EMPTY";
            return ss.str();
        }

        print_coords(ss, *this);
        return ss.str();
    }

    int GeoPolygon::num_loops() const {
        return _polygon->num_loops();
    }

    double GeoPolygon::get_area() const {
        return _polygon->GetArea();
    }

    S2Loop* GeoPolygon::get_loop(int i) const {
        return _polygon->loop(i);
    }

    std::size_t GeoPolygon::get_num_point() const {
        if(is_empty()) return 0;
        return _polygon->num_vertices() + _polygon->num_loops();
    }

    std::unique_ptr<GeoShape> GeoPolygon::boundary() const {
        std::unique_ptr<GeoMultiLineString> multi_linestring = GeoMultiLineString::create_unique();
        if(is_empty()){
            multi_linestring->set_empty();
            return multi_linestring;
        }

        std::unique_ptr<GeoCoordinateLists> coords_list =  to_coords();

        if(coords_list->coords_list.size() == 1){
            std::unique_ptr<GeoLineString> linestring = GeoLineString::create_unique();
            linestring->from_coords(*coords_list->coords_list[0]);
            return linestring;
        }

        for (int i = 0; i < coords_list->coords_list.size(); ++i) {
            std::unique_ptr<GeoLineString> linestring = GeoLineString::create_unique();
            linestring->from_coords(*coords_list->coords_list[i]);
            multi_linestring->add_one_geometry(linestring.release());
        }

        return multi_linestring;
    }

    bool GeoPolygon::add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const {
        if(is_empty() || !is_valid()) return false;
        S2shape_index.Add(std::make_unique<S2Polygon::Shape>(polygon()));
        return true;
    }


    void GeoPolygon::encode(std::string* buf, size_t& data_size) {
        Encoder encoder;
        _polygon->Encode(&encoder);
        data_size = encoder.length();
        buf->append(encoder.base(), encoder.length());
    }

    bool GeoPolygon::decode(const void* data, size_t size) {
        Decoder decoder(data, size);
        _polygon.reset(new S2Polygon());
        return _polygon->Decode(&decoder) && _polygon->IsValid();
    }

}// namespace doris
