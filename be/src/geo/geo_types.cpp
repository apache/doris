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

#include "geo/geo_types.h"

#include <absl/strings/str_format.h>
#include <glog/logging.h>
#include <s2/s1angle.h>
#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/util/coding/coder.h>
#include <s2/util/units/length-units.h>
#include <string.h>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <utility>
#include <vector>

#include "geo/geo_tobinary.h"
#include "geo/wkb_parse.h"
#include "geo/wkt_parse.h"

namespace doris {

GeoPoint::GeoPoint() : _point(new S2Point()) {}
GeoPoint::~GeoPoint() = default;

GeoLine::GeoLine() = default;
GeoLine::~GeoLine() = default;

GeoPolygon::GeoPolygon() = default;
GeoPolygon::~GeoPolygon() = default;

GeoCircle::GeoCircle() = default;
GeoCircle::~GeoCircle() = default;

void print_s2point(std::ostream& os, const S2Point& point) {
    S2LatLng coord(point);
    os << std::setprecision(12) << coord.lng().degrees() << " " << coord.lat().degrees();
}

static inline bool is_valid_lng_lat(double lng, double lat) {
    return std::abs(lng) <= 180 && std::abs(lat) <= 90;
}

// Return GEO_PARSE_OK, if and only if this can be converted to a valid S2Point
static inline GeoParseStatus to_s2point(double lng, double lat, S2Point* point) {
    if (!is_valid_lng_lat(lng, lat)) {
        return GEO_PARSE_COORD_INVALID;
    }
    S2LatLng ll = S2LatLng::FromDegrees(lat, lng);
    DCHECK(ll.is_valid()) << "invalid point, lng=" << lng << ", lat=" << lat;
    *point = ll.ToPoint();
    return GEO_PARSE_OK;
}

static inline GeoParseStatus to_s2point(const GeoCoordinate& coord, S2Point* point) {
    return to_s2point(coord.x, coord.y, point);
}

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

static GeoParseStatus to_s2loop(const GeoCoordinateList& coords, std::unique_ptr<S2Loop>* loop) {
    // 1. convert all coordinates to points
    std::vector<S2Point> points(coords.list.size());
    for (int i = 0; i < coords.list.size(); ++i) {
        auto res = to_s2point(coords.list[i], &points[i]);
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

static GeoParseStatus to_s2polyline(const GeoCoordinateList& coords,
                                    std::unique_ptr<S2Polyline>* polyline) {
    // 1. convert all coordinates to points
    std::vector<S2Point> points(coords.list.size());
    for (int i = 0; i < coords.list.size(); ++i) {
        auto res = to_s2point(coords.list[i], &points[i]);
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

static GeoParseStatus to_s2polygon(const GeoCoordinateListList& coords_list,
                                   std::unique_ptr<S2Polygon>* polygon) {
    std::vector<std::unique_ptr<S2Loop>> loops(coords_list.list.size());
    for (int i = 0; i < coords_list.list.size(); ++i) {
        auto res = to_s2loop(*coords_list.list[i], &loops[i]);
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

bool GeoShape::decode_from(const void* data, size_t size) {
    if (size < 2) {
        return false;
    }
    char reserved_byte = ((const char*)data)[0];
    char type_byte = ((const char*)data)[1];
    if (reserved_byte != 0X00 || type_byte != type()) {
        return false;
    }
    return decode((const char*)data + 2, size - 2);
}

void GeoShape::encode_to(std::string* buf) {
    // reserve a byte for future use
    buf->push_back(0X00);
    buf->push_back((char)type());
    encode(buf);
}

GeoShape* GeoShape::from_wkt(const char* data, size_t size, GeoParseStatus* status) {
    GeoShape* shape = nullptr;
    *status = WktParse::parse_wkt(data, size, &shape);
    return shape;
}

GeoShape* GeoShape::from_wkb(const char* data, size_t size, GeoParseStatus* status) {
    std::stringstream wkb;

    for (int i = 0; i < size; ++i) {
        if ((i == 1 && wkb.str() == "x") || (i == 2 && wkb.str() == "\\x")) {
            wkb.str(std::string());
        }
        wkb << *data;
        data++;
    }
    GeoShape* shape = nullptr;
    *status = WkbParse::parse_wkb(wkb, &shape);
    return shape;
}

std::unique_ptr<GeoShape> GeoShape::from_encoded(const void* ptr, size_t size) {
    if (size < 2 || ((const char*)ptr)[0] != 0X00) {
        return nullptr;
    }
    std::unique_ptr<GeoShape> shape;
    switch (((const char*)ptr)[1]) {
    case GEO_SHAPE_POINT: {
        shape = GeoPoint::create_unique();
        break;
    }
    case GEO_SHAPE_LINE_STRING: {
        shape = GeoLine::create_unique();
        break;
    }
    case GEO_SHAPE_POLYGON: {
        shape = GeoPolygon::create_unique();
        break;
    }
    case GEO_SHAPE_CIRCLE: {
        shape = GeoCircle::create_unique();
        break;
    }
    default:
        return nullptr;
    }
    auto res = shape->decode((const char*)ptr + 2, size - 2);
    if (!res) {
        return nullptr;
    }
    return shape;
}

GeoParseStatus GeoPoint::from_coord(double x, double y) {
    return to_s2point(x, y, _point.get());
}

GeoParseStatus GeoPoint::from_coord(const GeoCoordinate& coord) {
    return to_s2point(coord, _point.get());
}

GeoCoordinateList GeoPoint::to_coords() const {
    GeoCoordinate coord;
    coord.x = GeoPoint::x();
    coord.y = GeoPoint::y();
    GeoCoordinateList coords;
    coords.add(coord);
    return coords;
}

GeoCoordinateList GeoLine::to_coords() const {
    GeoCoordinateList coords;
    for (int i = 0; i < GeoLine::numPoint(); ++i) {
        GeoCoordinate coord;
        coord.x = std::stod(
                absl::StrFormat("%.13f", S2LatLng::Longitude(*GeoLine::getPoint(i)).degrees()));
        coord.y = std::stod(
                absl::StrFormat("%.13f", S2LatLng::Latitude(*GeoLine::getPoint(i)).degrees()));
        coords.add(coord);
    }
    return coords;
}

const std::unique_ptr<GeoCoordinateListList> GeoPolygon::to_coords() const {
    std::unique_ptr<GeoCoordinateListList> coordss(new GeoCoordinateListList());
    for (int i = 0; i < GeoPolygon::numLoops(); ++i) {
        std::unique_ptr<GeoCoordinateList> coords(new GeoCoordinateList());
        S2Loop* loop = GeoPolygon::getLoop(i);
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

std::string GeoPoint::to_string() const {
    return as_wkt();
}

void GeoPoint::encode(std::string* buf) {
    buf->append((const char*)_point.get(), sizeof(*_point));
}

bool GeoPoint::decode(const void* data, size_t size) {
    if (size != sizeof(*_point)) {
        return false;
    }
    memcpy(_point.get(), data, size);
    return true;
}

double GeoPoint::x() const {
    //Accurate to 13 decimal places
    return std::stod(absl::StrFormat("%.13f", S2LatLng::Longitude(*_point).degrees()));
}

double GeoPoint::y() const {
    //Accurate to 13 decimal places
    return std::stod(absl::StrFormat("%.13f", S2LatLng::Latitude(*_point).degrees()));
}

std::string GeoPoint::as_wkt() const {
    std::stringstream ss;
    ss << "POINT (";
    print_s2point(ss, *_point);
    ss << ")";
    return ss.str();
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

GeoParseStatus GeoLine::from_coords(const GeoCoordinateList& list) {
    return to_s2polyline(list, &_polyline);
}

void GeoLine::encode(std::string* buf) {
    Encoder encoder;
    _polyline->Encode(&encoder);
    buf->append(encoder.base(), encoder.length());
}

bool GeoLine::decode(const void* data, size_t size) {
    Decoder decoder(data, size);
    _polyline.reset(new S2Polyline());
    return _polyline->Decode(&decoder);
}

int GeoLine::numPoint() const {
    return _polyline->num_vertices();
}

S2Point* GeoLine::getPoint(int i) const {
    return const_cast<S2Point*>(&(_polyline->vertex(i)));
}

GeoParseStatus GeoPolygon::from_coords(const GeoCoordinateListList& list) {
    return to_s2polygon(list, &_polygon);
}

void GeoPolygon::encode(std::string* buf) {
    Encoder encoder;
    _polygon->Encode(&encoder);
    buf->append(encoder.base(), encoder.length());
}

bool GeoPolygon::decode(const void* data, size_t size) {
    Decoder decoder(data, size);
    _polygon.reset(new S2Polygon());
    return _polygon->Decode(&decoder) && _polygon->IsValid();
}

std::string GeoLine::as_wkt() const {
    std::stringstream ss;
    ss << "LINESTRING (";
    for (int i = 0; i < _polyline->num_vertices(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        print_s2point(ss, _polyline->vertex(i));
    }
    ss << ")";
    return ss.str();
}

std::string GeoPolygon::as_wkt() const {
    std::stringstream ss;
    ss << "POLYGON (";
    for (int i = 0; i < _polygon->num_loops(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << "(";
        const S2Loop* loop = _polygon->loop(i);
        for (int j = 0; j < loop->num_vertices(); ++j) {
            if (j != 0) {
                ss << ", ";
            }
            print_s2point(ss, loop->vertex(j));
        }
        ss << ", ";
        print_s2point(ss, loop->vertex(0));
        ss << ")";
    }
    ss << ")";

    return ss.str();
}

bool GeoPolygon::contains(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = (const GeoPoint*)rhs;
        return _polygon->Contains(*point->point());
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = (const GeoLine*)rhs;
        return _polygon->Contains(*line->polyline());
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = (const GeoPolygon*)rhs;
        return _polygon->Contains(*other->polygon());
    }
    default:
        return false;
    }
}

std::double_t GeoPolygon::getArea() const {
    return _polygon->GetArea();
}

int GeoPolygon::numLoops() const {
    return _polygon->num_loops();
}

S2Loop* GeoPolygon::getLoop(int i) const {
    return const_cast<S2Loop*>(_polygon->loop(i));
}

GeoParseStatus GeoCircle::init(double lng, double lat, double radius_meter) {
    S2Point center;
    auto status = to_s2point(lng, lat, &center);
    if (status != GEO_PARSE_OK) {
        return status;
    }
    S1Angle radius = S2Earth::ToAngle(util::units::Meters(radius_meter));
    _cap.reset(new S2Cap(center, radius));
    if (!_cap->is_valid()) {
        return GEO_PARSE_CIRCLE_INVALID;
    }
    return GEO_PARSE_OK;
}

bool GeoCircle::contains(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = (const GeoPoint*)rhs;
        return _cap->Contains(*point->point());
    }
    default:
        return false;
    }
}

void GeoCircle::encode(std::string* buf) {
    Encoder encoder;
    _cap->Encode(&encoder);
    buf->append(encoder.base(), encoder.length());
}

bool GeoCircle::decode(const void* data, size_t size) {
    Decoder decoder(data, size);
    _cap.reset(new S2Cap());
    return _cap->Decode(&decoder) && _cap->is_valid();
}

std::string GeoCircle::as_wkt() const {
    std::stringstream ss;
    ss << "CIRCLE ((";
    print_s2point(ss, _cap->center());
    ss << "), " << S2Earth::ToMeters(_cap->radius()) << ")";
    return ss.str();
}

double GeoCircle::getArea() const {
    return _cap->GetArea();
}

bool GeoShape::ComputeArea(GeoShape* rhs, double* area, std::string square_unit) {
    double steradians;
    switch (rhs->type()) {
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = (const GeoCircle*)rhs;
        steradians = circle->getArea();
        break;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = (const GeoPolygon*)rhs;
        steradians = polygon->getArea();
        break;
    }
    case GEO_SHAPE_POINT: {
        *area = 0;
        return true;
    }
    case GEO_SHAPE_LINE_STRING: {
        *area = 0;
        return true;
    }
    default:
        return false;
    }

    if (square_unit.compare("square_meters") == 0) {
        *area = S2Earth::SteradiansToSquareMeters(steradians);
        return true;
    } else if (square_unit.compare("square_km") == 0) {
        *area = S2Earth::SteradiansToSquareKm(steradians);
        return true;
    } else {
        return false;
    }
}

std::string GeoShape::as_binary(GeoShape* rhs) {
    std::string res;
    if (toBinary::geo_tobinary(rhs, &res)) {
        return res;
    }
    return res;
}

} // namespace doris
