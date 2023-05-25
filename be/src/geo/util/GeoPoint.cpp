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

#include "GeoPoint.h"
#include <absl/strings/str_format.h>
#include <s2/s2latlng.h>
#include <s2/s2earth.h>
#include <s2/mutable_s2shape_index.h>
#include <s2/s2point_vector_shape.h>
#include <s2/util/coding/coder.h>
#include <iomanip>
#include <sstream>

namespace doris {
    GeoPoint::GeoPoint() : _point(new S2Point()) {}
    GeoPoint::~GeoPoint() = default;

    void GeoPoint::print_s2point(std::ostream& os, const S2Point& point) {
        S2LatLng coord(point);
        os << std::setprecision(12) << coord.lng().degrees() << " " << coord.lat().degrees();
    }

    static inline bool is_valid_lng_lat(double lng, double lat) {
        return std::abs(lng) <= 180 && std::abs(lat) <= 90;
    }

    bool GeoPoint::is_valid() const {
        return is_valid_lng_lat(x(),y());
    }

    // Return GEO_PARSE_OK, if and only if this can be converted to a valid S2Point
     inline GeoParseStatus GeoPoint::to_s2point(double lng, double lat, S2Point* point) {
        if (!is_valid_lng_lat(lng, lat)) {
            return GEO_PARSE_COORD_INVALID;
        }
        S2LatLng ll = S2LatLng::FromDegrees(lat, lng);
        DCHECK(ll.is_valid()) << "invalid point, lng=" << lng << ", lat=" << lat;
        *point = ll.ToPoint();
        return GEO_PARSE_OK;
    }

    inline GeoParseStatus GeoPoint::to_s2point(const GeoCoordinate& coord, S2Point* point) {
        return to_s2point(coord.x, coord.y, point);
    }

    GeoParseStatus GeoPoint::from_coord(double x, double y) {
        return to_s2point(x, y, _point.get());
    }

    GeoParseStatus GeoPoint::from_coord(const GeoCoordinate& coord) {
        return to_s2point(coord, _point.get());
    }

    GeoParseStatus GeoPoint::from_s2point(S2Point* point){
        *_point = *point;
        return GEO_PARSE_OK;
    }

    //后面改成GeoCoordinate GeoPoint::to_coords(),现在涉及到wkb，暂时不动
    GeoCoordinates GeoPoint::to_coords() const {
        GeoCoordinate coord;
        coord.x = GeoPoint::x();
        coord.y = GeoPoint::y();
        GeoCoordinates coords;
        coords.add(coord);
        return coords;
    }

    std::unique_ptr<GeoCoordinate> GeoPoint::to_coord() const{
        return std::make_unique<GeoCoordinate>(GeoPoint::x(),GeoPoint::y());
    }

    std::unique_ptr<S2Shape> GeoPoint::get_s2shape() const{
        std::vector<S2Point> points;
        points.emplace_back(*point());
        return std::make_unique<S2PointVectorShape>(points);
    }

    bool GeoPoint::contains(const GeoShape* rhs) const {
        switch (rhs->type()) {
            case GEO_SHAPE_POINT: {
                const GeoPoint* point = (const GeoPoint*)rhs;
                return *_point == *(point->_point);
            }
            default:
                return false;
        }
    }

    bool GeoPoint::ComputeDistance(double x_lng, double x_lat, double y_lng, double y_lat,
                                   double* distance) {
        S2LatLng x = S2LatLng::FromDegrees(x_lat, x_lng);
        if (!x.is_valid()) {
            return false;
        }
        S2LatLng y = S2LatLng::FromDegrees(y_lat, y_lng);
        if (!y.is_valid()) {
            return false;
        }
        *distance = S2Earth::GetDistanceMeters(x, y);
        return true;
    }

    bool GeoPoint::ComputeAngleSphere(double x_lng, double x_lat, double y_lng, double y_lat,
                                      double* angle) {
        S2LatLng x = S2LatLng::FromDegrees(x_lat, x_lng);
        if (!x.is_valid()) {
            return false;
        }
        S2LatLng y = S2LatLng::FromDegrees(y_lat, y_lng);
        if (!y.is_valid()) {
            return false;
        }
        *angle = (x.GetDistance(y)).degrees();
        return true;
    }

    bool GeoPoint::ComputeAngle(GeoPoint* point1, GeoPoint* point2, GeoPoint* point3, double* angle) {
        S2LatLng latLng1 = S2LatLng::FromDegrees(point1->x(), point1->y());
        S2LatLng latLng2 = S2LatLng::FromDegrees(point2->x(), point2->y());
        S2LatLng latLng3 = S2LatLng::FromDegrees(point3->x(), point3->y());

        //If points 2 and 3 are the same or points 2 and 1 are the same, returns NULL.
        if (latLng2.operator==(latLng1) || latLng2.operator==(latLng3)) {
            return false;
        }
        double x = 0;
        double y = 0;
        //If points 2 and 3 are exactly antipodal or points 2 and 1 are exactly antipodal, returns NULL.
        if (GeoPoint::ComputeAngleSphere(point1->x(), point1->y(), point2->x(), point2->y(), &x) &&
            GeoPoint::ComputeAngleSphere(point3->x(), point3->y(), point2->x(), point2->y(), &y)) {
            if (x == 180 || y == 180) {
                return false;
            }
        } else {
            return false;
        }
        //Computes the initial bearing (radians) from latLng2 to latLng3
        double a = S2Earth::GetInitialBearing(latLng2, latLng3).radians();
        //Computes the initial bearing (radians) from latLng2 to latLng1
        double b = S2Earth::GetInitialBearing(latLng2, latLng1).radians();
        //range [0, 2pi)
        if (b - a < 0) {
            *angle = b - a + 2 * M_PI;
        } else {
            *angle = b - a;
        }
        return true;
    }

    bool GeoPoint::ComputeAzimuth(GeoPoint* p1, GeoPoint* p2, double* angle) {
        GeoPoint north;
        north.from_coord(0, 90);
        return GeoPoint::ComputeAngle(&north, p1, p2, angle);
    }

    std::string GeoPoint::as_wkt() const {
        std::stringstream ss;
        ss << "POINT ";
        if (is_empty()){
            ss << "EMPTY";
            return ss.str();
        }

        ss << "(";
        print_s2point(ss, *_point);
        ss << ")";
        return ss.str();
    }

    std::unique_ptr<GeoShape> GeoPoint::boundary() const {
        std::unique_ptr<GeoShape> point(GeoPoint::create_unique().release());
        point->set_empty();
        return point;
    }

    bool GeoPoint::add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const {
        if(is_empty() || !is_valid()) return false;
        std::vector<S2Point> points;
        points.emplace_back(*point());
        S2shape_index.Add(std::make_unique<S2PointVectorShape>(points));
        return true;
    }


    /*void GeoPoint::add_to_s2point_index(S2PointIndex<int>& S2point_index, int i){
        S2point_index.Add(*point(), i);
    }*/

    double GeoPoint::x() const {
        //Accurate to 13 decimal places
        return std::stod(absl::StrFormat("%.13f", S2LatLng::Longitude(*_point).degrees()));
    }

    double GeoPoint::y() const {
        //Accurate to 13 decimal places
        return std::stod(absl::StrFormat("%.13f", S2LatLng::Latitude(*_point).degrees()));
    }

    void GeoPoint::encode(std::string* buf, size_t& data_size) {
        data_size = sizeof(*_point);
        buf->append((const char*)_point.get(), sizeof(*_point));
    }

    bool GeoPoint::decode(const void* data, size_t size) {
        if (size != sizeof(*_point)) {
            return false;
        }
        memcpy(_point.get(), data, size);
        return true;
    }

} // namespace doris
