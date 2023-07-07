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

#include "GeoLineString.h"
#include <s2/s2polyline.h>
#include <s2/util/coding/coder.h>
#include <vector>
#include "GeoMultiPoint.h"

namespace doris {

    GeoLineString::GeoLineString() = default;
    GeoLineString::~GeoLineString() = default;

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

    bool GeoLineString::is_valid() const {
        return polyline()->IsValid();
    }

    bool GeoLineString::is_closed() const {
        return !is_empty() && polyline()->vertex(0) == polyline()->vertex(get_num_point() - 1);
    }

     GeoParseStatus GeoLineString::to_s2polyline(const GeoCoordinates& coords,
                                        std::unique_ptr<S2Polyline>* polyline) {
        // 1. convert all coordinates to points
        std::vector<S2Point> points(coords.coords.size());
        for (int i = 0; i < coords.coords.size(); ++i) {
            auto res = GeoPoint::to_s2point(coords.coords[i].x,coords.coords[i].y, &points[i]);
            if (res != GEO_PARSE_OK) {
                return res;
            }
        }
        // 2. remove duplicate points
        remove_duplicate_points(&points);
        // 3. check if there is enough point
        if (points.size() < 2) {
            return GEO_PARSE_POLYLINE_LACK_VERTICES;
        }
        polyline->reset(new S2Polyline(points));
        if (!(*polyline)->IsValid()) {
            return GEO_PARSE_POLYLINE_INVALID;
        }
        return GEO_PARSE_OK;
    }

    GeoParseStatus GeoLineString::from_coords(const GeoCoordinates& coords) {
        return to_s2polyline(coords, &_polyline);
    }

    GeoCoordinates GeoLineString::to_coords() const {
        GeoCoordinates coords;
        for (int i = 0; i < GeoLineString::num_point(); ++i) {
            GeoCoordinate coord;
            coord.x = std::stod(
                    absl::StrFormat("%.12f", S2LatLng::Longitude(*GeoLineString::get_point(i)).degrees()));
            coord.y = std::stod(
                    absl::StrFormat("%.12f", S2LatLng::Latitude(*GeoLineString::get_point(i)).degrees()));
            coords.add(coord);
        }
        return coords;
    }

    std::unique_ptr<GeoCoordinates> GeoLineString::to_coords_ptr() const {
        std::unique_ptr<GeoCoordinates> coords(new GeoCoordinates());
        for (int i = 0; i < GeoLineString::num_point(); ++i) {
            GeoCoordinate coord;
            coord.x = std::stod(
                    absl::StrFormat("%.12f", S2LatLng::Longitude(*GeoLineString::get_point(i)).degrees()));
            coord.y = std::stod(
                    absl::StrFormat("%.12f", S2LatLng::Latitude(*GeoLineString::get_point(i)).degrees()));
            coords->add(coord);
        }
        return coords;
    }

    double GeoLineString::length() const {
        return  _polyline->GetLength().radians();
    }

    std::unique_ptr<S2Shape> GeoLineString::get_s2shape() const{
        return std::make_unique<S2Polyline::Shape>(polyline());
    }

    std::string GeoLineString::as_wkt() const {
        std::stringstream ss;
        ss << "LINESTRING ";
        if (is_empty()){
            ss << "EMPTY";
            return ss.str();
        }
        ss << "(";
        for (int i = 0; i < _polyline->num_vertices(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            GeoPoint::print_s2point(ss, _polyline->vertex(i));
        }
        ss << ")";
        return ss.str();
    }

    int GeoLineString::num_point() const {
        return _polyline->num_vertices();
    }

    S2Point* GeoLineString::get_point(int i) const {
        return const_cast<S2Point*>(&(_polyline->vertex(i)));
    }

    void GeoLineString::encode(std::string* buf, size_t& data_size) {
        Encoder encoder;
        _polyline->Encode(&encoder);
        data_size = encoder.length();
        buf->append(encoder.base(), encoder.length());
    }

    bool GeoLineString::decode(const void* data, size_t size) {
        Decoder decoder(data, size);
        _polyline.reset(new S2Polyline());
        return _polyline->Decode(&decoder);
    }

    double GeoLineString::line_locate_point(GeoPoint* point){
        int num = num_point();
        return _polyline->UnInterpolate(_polyline->Project(*point->point(),&num),num) ;
    }

    std::size_t GeoLineString::get_num_point() const {
        if(is_empty()) return 0;
        return _polyline->num_vertices();
    }

    std::unique_ptr<GeoShape> GeoLineString::boundary() const {
        std::unique_ptr<GeoMultiPoint> multipoint = GeoMultiPoint::create_unique();
        if(is_empty() || is_ring()){
            multipoint->set_empty();
            return multipoint;
        }

        std::unique_ptr<GeoPoint> point1 = GeoPoint::create_unique();
        std::unique_ptr<GeoPoint> point2 = GeoPoint::create_unique();
        point1->from_s2point(get_point(0));
        point2->from_s2point(get_point(num_point()-1));

        if(point1->contains(point2.get())){
            multipoint->set_empty();
            return multipoint;
        }

        multipoint->add_one_geometry(point1.release());
        multipoint->add_one_geometry(point2.release());

        return multipoint;
    }

    bool GeoLineString::add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const {
        if(is_empty() || !is_valid()) return false;
        S2shape_index.Add(std::make_unique<S2Polyline::Shape>(polyline()));
        return true;
    }

} // namespace doris
