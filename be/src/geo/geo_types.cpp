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
#include <s2/s2builderutil_s2polygon_layer.h>
#include <s2/s2builderutil_s2polyline_vector_layer.h>
#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2edge_crosser.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/util/coding/coder.h>
#include <s2/util/units/length-units.h>
#include <string.h>

#include "vec/common/assert_cast.h"
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
#include "common/compile_check_avoid_begin.h"

constexpr double TOLERANCE = 1e-6;

GeoPoint::GeoPoint() : _point(new S2Point()) {}
GeoPoint::~GeoPoint() = default;

GeoLine::GeoLine() = default;
GeoLine::~GeoLine() = default;

GeoPolygon::GeoPolygon() = default;
GeoPolygon::~GeoPolygon() = default;

GeoCircle::GeoCircle() = default;
GeoCircle::~GeoCircle() = default;

GeoMultiPolygon::GeoMultiPolygon() = default;
GeoMultiPolygon::~GeoMultiPolygon() = default;

void print_s2point(std::ostream& os, const S2Point& point) {
    S2LatLng coord(point);
    os << std::setprecision(15) << coord.lng().degrees() << " " << coord.lat().degrees();
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

double project_distance(const S2Point& point, const S2Point& lineStart, const S2Point& lineEnd) {
    S2Point lineVector = lineEnd - lineStart;
    S2Point pointVector = point - lineStart;
    double lineVectorMagnitudeSquared = lineVector.DotProd(lineVector);
    double t = pointVector.DotProd(lineVector) / lineVectorMagnitudeSquared;
    t = t > 0 ? t : 0;
    t = t < 1 ? t : 1;
    S2Point nearestPoint = lineStart + t * lineVector;
    S2Point distanceVector = point - nearestPoint;
    return sqrt(distanceVector.DotProd(distanceVector));
}

double compute_distance_to_line(const S2Point& point, const S2Polyline* line) {
    const S2Point& line_point1 = line->vertex(0);
    const S2Point& line_point2 = line->vertex(1);
    S2LatLng lp1 = S2LatLng(line_point1);
    S2LatLng lp2 = S2LatLng(line_point2);

    S2LatLng lquery = S2LatLng(point);
    double lat1 = lp1.lat().degrees();
    double long1 = lp1.lng().degrees();

    double lat2 = lp2.lat().degrees();
    double long2 = lp2.lng().degrees();

    double latq = lquery.lat().degrees();
    double longq = lquery.lng().degrees();
    return project_distance({latq, longq, 0}, {lat1, long1, 0}, {lat2, long2, 0});
}

double compute_distance_to_point(const S2Point& point1, const S2Point& point2) {
    S2LatLng lp1 = S2LatLng(point1);
    S2LatLng lp2 = S2LatLng(point2);

    double lat1 = lp1.lat().degrees();
    double long1 = lp1.lng().degrees();

    double lat2 = lp2.lat().degrees();
    double long2 = lp2.lng().degrees();
    return sqrt((lat1 - lat2) * (lat1 - lat2) + (long1 - long2) * (long1 - long2));
}

double compute_distance_to_line(const S2Point& point, const S2Point& line_point1,
                                const S2Point& line_point2) {
    S2LatLng lp1 = S2LatLng(line_point1);
    S2LatLng lp2 = S2LatLng(line_point2);

    S2LatLng lquery = S2LatLng(point);
    double lat1 = lp1.lat().degrees();
    double long1 = lp1.lng().degrees();

    double lat2 = lp2.lat().degrees();
    double long2 = lp2.lng().degrees();

    double latq = lquery.lat().degrees();
    double longq = lquery.lng().degrees();
    return project_distance({latq, longq, 0}, {lat1, long1, 0}, {lat2, long2, 0});
}

double cross_product(const S2Point& a, const S2Point& b, const S2Point& c) {
    return (b.x() - a.x()) * (c.y() - a.y()) - (b.y() - a.y()) * (c.x() - a.x());
}

bool is_point_on_segment(const S2Point& p, const S2Point& a, const S2Point& b) {
    return (p.x() >= std::min(a.x(), b.x()) && p.x() <= std::max(a.x(), b.x()) &&
            p.y() >= std::min(a.y(), b.y()) && p.y() <= std::max(a.y(), b.y()) &&
            p.z() >= std::min(a.z(), b.z()) && p.z() <= std::max(a.z(), b.z()));
}

bool do_segments_intersect(const S2Point& a1, const S2Point& a2, const S2Point& b1,
                           const S2Point& b2) {
    if (std::max(a1.x(), a2.x()) < std::min(b1.x(), b2.x()) ||
        std::max(a1.y(), a2.y()) < std::min(b1.y(), b2.y()) ||
        std::max(a1.z(), a2.z()) < std::min(b1.z(), b2.z()) ||
        std::min(a1.x(), a2.x()) > std::max(b1.x(), b2.x()) ||
        std::min(a1.y(), a2.y()) > std::max(b1.y(), b2.y()) ||
        std::min(a1.z(), a2.z()) > std::max(b1.z(), b2.z())) {
        return false;
    }

    double d1 = cross_product(b1, b2, a1);
    double d2 = cross_product(b1, b2, a2);
    double d3 = cross_product(a1, a2, b1);
    double d4 = cross_product(a1, a2, b2);

    if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) {
        if ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
            return true;
        }
    }

    if (d1 == 0 && is_point_on_segment(a1, b1, b2)) {
        return true;
    }
    if (d2 == 0 && is_point_on_segment(a2, b1, b2)) {
        return true;
    }
    if (d3 == 0 && is_point_on_segment(b1, a1, a2)) {
        return true;
    }
    if (d4 == 0 && is_point_on_segment(b2, a1, a2)) {
        return true;
    }

    return false;
}

bool is_segments_intersect(const S2Point& point1, const S2Point& point2, const S2Point& line_point1,
                           const S2Point& line_point2) {
    S2LatLng lp1 = S2LatLng(line_point1);
    S2LatLng lp2 = S2LatLng(line_point2);

    S2LatLng lquery1 = S2LatLng(point1);
    S2LatLng lquery2 = S2LatLng(point2);
    double lat1 = lp1.lat().degrees();
    double long1 = lp1.lng().degrees();

    double lat2 = lp2.lat().degrees();
    double long2 = lp2.lng().degrees();

    double latq1 = lquery1.lat().degrees();
    double longq1 = lquery1.lng().degrees();
    double latq2 = lquery2.lat().degrees();
    double longq2 = lquery2.lng().degrees();
    return do_segments_intersect({latq1, longq1, 0}, {latq2, longq2, 0}, {lat1, long1, 0},
                                 {lat2, long2, 0});
}

static bool ray_crosses_segment(double px, double py, double ax, double ay, double bx, double by) {
    if (ay > by) {
        std::swap(ax, bx);
        std::swap(ay, by);
    }

    if (py <= ay || py > by) return false;
    double intersectX;
    if (std::abs(ax - bx) < std::numeric_limits<double>::epsilon()) {
        intersectX = ax;
    } else {
        double slope = (bx - ax) / (by - ay);
        intersectX = ax + slope * (py - ay);
    }
    return px < intersectX;
}

bool is_point_in_polygon(const S2Point& point, const S2Polygon* polygon) {
    int crossings = 0;

    for (int j = 0; j < polygon->num_loops(); ++j) {
        const S2Loop* loop = polygon->loop(j);
        for (int k = 0; k < loop->num_vertices(); ++k) {
            const S2Point& p1 = loop->vertex(k);
            const S2Point& p2 = loop->vertex((k + 1) % loop->num_vertices());

            S2LatLng lp1 = S2LatLng(p1);
            S2LatLng lp2 = S2LatLng(p2);

            S2LatLng lquery = S2LatLng(point);
            double lat1 = lp1.lat().degrees();
            double long1 = lp1.lng().degrees();

            double lat2 = lp2.lat().degrees();
            double long2 = lp2.lng().degrees();

            double latq = lquery.lat().degrees();
            double longq = lquery.lng().degrees();

            bool crossesRay = ray_crosses_segment(latq, longq, lat1, long1, lat2, long2);

            if (crossesRay) {
                crossings++;
            }
        }
    }
    return (crossings % 2 == 1);
}

bool is_line_touches_line(const S2Point& Line1_Point1, const S2Point& Line1_Point2,
                          const S2Point& Line2_Point1, const S2Point& Line2_Point2) {
    int count = 0;
    if (compute_distance_to_line(Line1_Point1, Line2_Point1, Line2_Point2) < TOLERANCE) {
        count++;
    }
    if (compute_distance_to_line(Line1_Point2, Line2_Point1, Line2_Point2) < TOLERANCE) {
        count++;
    }
    if (compute_distance_to_line(Line2_Point1, Line1_Point1, Line1_Point2) < TOLERANCE) {
        count++;
    }
    if (compute_distance_to_line(Line2_Point2, Line1_Point1, Line1_Point2) < TOLERANCE) {
        count++;
    }
    // Two intersections are allowed when there is only one intersection, or when the intersection is an endpoint
    if (count == 1 ||
        (count == 2 && ((Line1_Point1 == Line2_Point1 || Line1_Point1 == Line2_Point2) +
                        (Line1_Point2 == Line2_Point1 || Line1_Point2 == Line2_Point2)) == 1)) {
        return true;
    }
    return false;
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
    case GEO_SHAPE_MULTI_POLYGON: {
        shape = GeoMultiPolygon::create_unique();
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

const std::vector<std::unique_ptr<GeoCoordinateListList>> GeoMultiPolygon::to_coords() const {
    std::vector<std::unique_ptr<GeoCoordinateListList>> coordss;
    for (const auto& polygon : _polygons) {
        std::unique_ptr<GeoCoordinateListList> coords = polygon->to_coords();
        coordss.push_back(std::move(coords));
    }
    return coordss;
}

bool GeoPoint::intersects(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        // points and points are considered to intersect when they are equal
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        return *_point == *point->point();
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        return line->intersects(this);
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = assert_cast<const GeoPolygon*>(rhs);
        return polygon->intersects(this);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        return multi_polygon->intersects(this);
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->intersects(this);
    }
    default:
        return false;
    }
}

bool GeoPoint::disjoint(const GeoShape* rhs) const {
    return !intersects(rhs);
}

bool GeoPoint::touches(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        // always returns false because the point has no boundaries
        return false;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        return line->touches(this);
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = assert_cast<const GeoPolygon*>(rhs);
        return polygon->touches(this);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        return multi_polygon->touches(this);
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->touches(this);
    }
    default:
        return false;
    }
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

bool GeoLine::intersects(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        return compute_distance_to_line(*point->point(), _polyline.get()) < TOLERANCE;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        return is_segments_intersect(_polyline->vertex(0), _polyline->vertex(1),
                                     line->polyline()->vertex(0), line->polyline()->vertex(1));
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = assert_cast<const GeoPolygon*>(rhs);
        return polygon->polygon()->Intersects(*_polyline);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        for (const auto& polygon : multi_polygon->polygons()) {
            if (polygon->intersects(this)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->intersects(this);
    }
    default:
        return false;
    }
}

bool GeoLine::disjoint(const GeoShape* rhs) const {
    return !intersects(rhs);
}

bool GeoLine::touches(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        const S2Point& start = _polyline->vertex(0);
        const S2Point& end = _polyline->vertex(_polyline->num_vertices() - 1);
        // 1. Points do not have boundaries. when the point is on the start or end of the line return true
        if (start == *point->point() || end == *point->point()) {
            return true;
        }
        return false;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* other = assert_cast<const GeoLine*>(rhs);

        const S2Point& p1 = _polyline->vertex(0);
        const S2Point& p2 = _polyline->vertex(1);

        const S2Point& p3 = other->polyline()->vertex(0);
        const S2Point& p4 = other->polyline()->vertex(1);
        return is_line_touches_line(p1, p2, p3, p4);
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = assert_cast<const GeoPolygon*>(rhs);
        return polygon->touches(this);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        return multi_polygon->touches(this);
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->touches(this);
    }
    default:
        return false;
    }
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
    // A third-party dependency is used that returns a const S2Point&,
    // but it's not itself a const, so it's OK to use a const_cast
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

bool GeoPolygon::intersects(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        if (is_point_in_polygon(*point->point(), _polygon.get())) {
            return true;
        }
        return polygon_touch_point(_polygon.get(), point->point());
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        std::vector<std::unique_ptr<S2Polyline>> intersect_lines =
                _polygon->IntersectWithPolyline(*line->polyline());
        if (!intersect_lines.empty()) {
            return true;
        }

        for (int i = 0; i < line->polyline()->num_vertices(); i++) {
            const S2Point& outer_p1 = line->polyline()->vertex(i);
            const S2Point& outer_p2 =
                    line->polyline()->vertex((i + 1) % line->polyline()->num_vertices());
            for (int j = 0; j < _polygon->num_loops(); ++j) {
                const S2Loop* loop = _polygon->loop(j);
                for (int k = 0; k < loop->num_vertices(); ++k) {
                    const S2Point& p1 = loop->vertex(k);
                    const S2Point& p2 = loop->vertex((k + 1) % loop->num_vertices());
                    if (is_segments_intersect(outer_p1, outer_p2, p1, p2)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = assert_cast<const GeoPolygon*>(rhs);
        // When two polygons intersect only at the boundary, s2geometry may not return the correct result.
        if (!_polygon->Intersects(*other->polygon())) {
            return polygon_touch_polygon(_polygon.get(), other->polygon());
        }
        return true;
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        for (const auto& other : multi_polygon->polygons()) {
            if (this->intersects(other.get())) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->intersects(this);
    }
    default:
        return false;
    }
}

bool GeoPolygon::disjoint(const GeoShape* rhs) const {
    return !intersects(rhs);
}

bool GeoPolygon::polygon_touch_point(const S2Polygon* polygon, const S2Point* point) const {
    for (int k = 0; k < polygon->num_loops(); ++k) {
        const S2Loop* innee_loop = polygon->loop(k);
        for (int l = 0; l < innee_loop->num_vertices(); ++l) {
            const S2Point& p1 = innee_loop->vertex(l);
            const S2Point& p2 = innee_loop->vertex((l + 1) % innee_loop->num_vertices());
            double distance = compute_distance_to_line(*point, p1, p2);
            if (distance < TOLERANCE) {
                return true;
            }
        }
    }
    return false;
}

bool GeoPolygon::polygon_touch_polygon(const S2Polygon* polygon1, const S2Polygon* polygon2) const {
    // Dual-check to avoid the following situations
    // POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))
    // POLYGON((5 10, 10 5, 15 10, 10 15, 5 10))
    for (int i = 0; i < polygon1->num_loops(); ++i) {
        const S2Loop* loop = polygon1->loop(i);
        for (int j = 0; j < loop->num_vertices(); ++j) {
            const S2Point& p1 = loop->vertex(j);
            const S2Point& p2 = loop->vertex((j + 1) % loop->num_vertices());
            for (int k = 0; k < polygon2->num_loops(); ++k) {
                const S2Loop* innee_loop = polygon2->loop(k);
                for (int l = 0; l < innee_loop->num_vertices(); ++l) {
                    const S2Point& p3 = innee_loop->vertex(l);
                    const S2Point& p4 = innee_loop->vertex((l + 1) % innee_loop->num_vertices());
                    if (compute_distance_to_line(p1, p3, p4) < TOLERANCE ||
                        compute_distance_to_line(p2, p3, p4) < TOLERANCE ||
                        compute_distance_to_line(p3, p1, p2) < TOLERANCE ||
                        compute_distance_to_line(p4, p1, p2) < TOLERANCE) {
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

bool GeoPolygon::touches(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        return polygon_touch_point(_polygon.get(), point->point());
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        std::vector<std::unique_ptr<S2Polyline>> intersect_lines =
                _polygon->IntersectWithPolyline(*line->polyline());

        std::set<S2Point> polygon_points;
        // 1. collect all points in the polygon
        for (int i = 0; i < _polygon->num_loops(); ++i) {
            const S2Loop* loop = _polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                const S2Point& p = loop->vertex(j);
                polygon_points.insert(p);
            }
        }
        // 2. check if the intersect line's points are on the polygon
        for (auto& iline : intersect_lines) {
            for (int i = 0; i < iline->num_vertices(); ++i) {
                const S2Point& p = iline->vertex(i);
                if (polygon_points.find(p) == polygon_points.end()) {
                    return false;
                }
            }
        }
        // 3. check if the line is on the boundary of the polygon
        if (intersect_lines.empty()) {
            for (const S2Point& p : polygon_points) {
                double distance = compute_distance_to_line(p, line->polyline());
                if (distance < TOLERANCE) {
                    return true;
                }
            }
            for (int i = 0; i < line->polyline()->num_vertices(); i++) {
                const S2Point& p = line->polyline()->vertex(i);
                for (int j = 0; j < _polygon->num_loops(); ++j) {
                    const S2Loop* loop = _polygon->loop(j);
                    for (int k = 0; k < loop->num_vertices(); ++k) {
                        const S2Point& p1 = loop->vertex(k);
                        const S2Point& p2 = loop->vertex((k + 1) % loop->num_vertices());
                        double distance = compute_distance_to_line(p, p1, p2);
                        if (distance < TOLERANCE) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
        return true;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = assert_cast<const GeoPolygon*>(rhs);
        const S2Polygon* polygon1 = _polygon.get();
        const S2Polygon* polygon2 = other->polygon();

        // "Touches" equivalent to boundary contact  but no internal overlap
        std::unique_ptr<S2Polygon> intersection(new S2Polygon());
        intersection->InitToIntersection(*polygon1, *polygon2);
        return (intersection->GetArea() < S1Angle::Radians(TOLERANCE).radians() &&
                polygon_touch_polygon(polygon1, polygon2));
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        bool has_touches = false;
        for (const auto& other : multi_polygon->polygons()) {
            if (this->intersects(other.get())) {
                if (!this->touches(other.get())) {
                    return false;
                }
                has_touches = true;
            }
        }
        return has_touches;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->touches(this);
    }
    default:
        return false;
    }
}

bool GeoPolygon::contains(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = (const GeoPoint*)rhs;
        if (!_polygon->Contains(*point->point())) {
            return false;
        }

        // Point on the edge of polygon doesn't count as "Contians"
        for (int i = 0; i < _polygon->num_loops(); ++i) {
            const S2Loop* loop = _polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                const S2Point& p1 = loop->vertex(j);
                const S2Point& p2 = loop->vertex((j + 1) % loop->num_vertices());
                if (compute_distance_to_line(*point->point(), p1, p2) < TOLERANCE) {
                    return false;
                }
            }
        }
        return true;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = (const GeoLine*)rhs;

        // solve the problem caused by the `Contains(const S2Polyline)` in the S2 library
        // due to the direction of the line segment
        for (int i = 0; i < _polygon->num_loops(); ++i) {
            const S2Loop* loop = _polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                const S2Point& p1 = loop->vertex(j);
                const S2Point& p2 = loop->vertex((j + 1) % loop->num_vertices());
                for (int k = 0; k < line->polyline()->num_vertices() - 1; ++k) {
                    const S2Point& p3 = line->polyline()->vertex(k);
                    const S2Point& p4 = line->polyline()->vertex(k + 1);
                    if ((compute_distance_to_line(p3, p1, p2) < TOLERANCE ||
                         compute_distance_to_line(p4, p1, p2) < TOLERANCE) &&
                        !is_line_touches_line(p1, p2, p3, p4)) {
                        return false;
                    }
                }
            }
        }

        return _polygon->Contains(*line->polyline());
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = (const GeoPolygon*)rhs;
        return _polygon->Contains(*other->polygon());
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = (const GeoMultiPolygon*)rhs;
        for (const auto& other : multi_polygon->polygons()) {
            if (!this->contains(other.get())) {
                return false;
            }
        }
        return true;
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
    return _polygon->loop(i);
}

GeoParseStatus GeoMultiPolygon::from_coords(const std::vector<GeoCoordinateListList>& list) {
    _polygons.clear();
    for (const auto& coords_list : list) {
        std::unique_ptr<GeoPolygon> polygon = GeoPolygon::create_unique();
        auto status = polygon->from_coords(coords_list);
        if (status != GEO_PARSE_OK) {
            return status;
        }
        _polygons.push_back(std::move(polygon));
    }

    return check_self_intersection();
}

GeoParseStatus GeoMultiPolygon::check_self_intersection() {
    for (int i = 0; i < _polygons.size(); ++i) {
        for (int j = i + 1; j < _polygons.size(); ++j) {
            if (_polygons[i]->intersects(_polygons[j].get())) {
                if (!_polygons[i]->touches(_polygons[j].get())) {
                    return GEO_PARSE_MULTIPOLYGON_OVERLAP;
                }
            } else {
                continue;
            }

            //  Polygons in a multipolygon can only share discrete points, not edges.
            for (int k = 0; k < _polygons[i]->numLoops(); ++k) {
                const S2Loop* loop1 = _polygons[i]->getLoop(k);
                for (int l = 0; l < _polygons[j]->numLoops(); ++l) {
                    const S2Loop* loop2 = _polygons[j]->getLoop(l);
                    for (int m = 0; m < loop1->num_vertices(); ++m) {
                        const S2Point& p1 = loop1->vertex(m);
                        const S2Point& p2 = loop1->vertex((m + 1) % loop1->num_vertices());
                        for (int n = 0; n < loop2->num_vertices(); ++n) {
                            const S2Point& p3 = loop2->vertex(n);
                            const S2Point& p4 = loop2->vertex((n + 1) % loop2->num_vertices());

                            // 1. At least one endpoint of an edge is near another edge
                            // 2. Check the edges "touches" each other in a valid way
                            if ((compute_distance_to_line(p1, p3, p4) < TOLERANCE ||
                                 compute_distance_to_line(p2, p3, p4) < TOLERANCE ||
                                 compute_distance_to_line(p3, p1, p2) < TOLERANCE ||
                                 compute_distance_to_line(p4, p1, p2) < TOLERANCE) &&
                                !is_line_touches_line(p1, p2, p3, p4)) {
                                return GEO_PARSE_MULTIPOLYGON_OVERLAP;
                            }
                        }
                    }
                }
            }
        }
    }
    return GEO_PARSE_OK;
}

bool GeoMultiPolygon::intersects(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->intersects(point)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->intersects(line)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = assert_cast<const GeoPolygon*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->intersects(other)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        for (const auto& other : multi_polygon->polygons()) {
            for (const auto& polygon : this->_polygons) {
                if (polygon->intersects(other.get())) {
                    return true;
                }
            }
        }
        return false;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->intersects(this);
    }
    default:
        return false;
    }
}

bool GeoMultiPolygon::disjoint(const GeoShape* rhs) const {
    return !intersects(rhs);
}

bool GeoMultiPolygon::touches(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->touches(point)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        bool has_touches = false;
        for (const auto& polygon : this->_polygons) {
            if (polygon->intersects(line)) {
                if (!polygon->touches(line)) {
                    return false;
                }
                has_touches = true;
            }
        }
        return has_touches;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = assert_cast<const GeoPolygon*>(rhs);
        bool has_touches = false;
        for (const auto& polygon : this->_polygons) {
            if (polygon->intersects(other)) {
                if (!polygon->touches(other)) {
                    return false;
                }
                has_touches = true;
            }
        }
        return has_touches;
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        bool has_touches = false;
        for (const auto& other : multi_polygon->polygons()) {
            for (const auto& polygon : this->_polygons) {
                if (polygon->intersects(other.get())) {
                    if (!polygon->touches(other.get())) {
                        return false;
                    }
                    has_touches = true;
                }
            }
        }
        return has_touches;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        return circle->touches(this);
    }
    default:
        return false;
    }
}

bool GeoMultiPolygon::contains(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->contains(point)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->contains(line)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* other = assert_cast<const GeoPolygon*>(rhs);
        for (const auto& polygon : this->_polygons) {
            if (polygon->contains(other)) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        //All polygons in rhs need to be contained
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        for (const auto& other : multi_polygon->polygons()) {
            bool is_contains = false;
            for (const auto& polygon : this->_polygons) {
                if (polygon->contains(other.get())) {
                    is_contains = true;
                    break;
                }
            }
            if (!is_contains) {
                return false;
            }
        }
        return true;
    }
    default:
        return false;
    }
}

std::string GeoMultiPolygon::as_wkt() const {
    std::stringstream ss;
    ss << "MULTIPOLYGON (";
    for (size_t i = 0; i < _polygons.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << "(";
        const S2Polygon* polygon = _polygons[i]->polygon();
        for (int j = 0; j < polygon->num_loops(); ++j) {
            if (j != 0) {
                ss << ", ";
            }
            ss << "(";
            const S2Loop* loop = polygon->loop(j);
            for (int k = 0; k < loop->num_vertices(); ++k) {
                if (k != 0) {
                    ss << ", ";
                }
                print_s2point(ss, loop->vertex(k));
            }
            ss << ", ";
            print_s2point(ss, loop->vertex(0));
            ss << ")";
        }
        ss << ")";
    }
    ss << ")";
    return ss.str();
}

double GeoMultiPolygon::getArea() const {
    double area = 0;
    for (const auto& polygon : _polygons) {
        area += polygon->getArea();
    }
    return area;
}

void GeoMultiPolygon::encode(std::string* buf) {
    Encoder encoder;
    encoder.Ensure(sizeof(size_t));
    encoder.put_varint32(_polygons.size());
    for (const auto& polygon : _polygons) {
        polygon->polygon()->Encode(&encoder);
    }
    buf->append(encoder.base(), encoder.length());
}

bool GeoMultiPolygon::decode(const void* data, size_t size) {
    Decoder decoder(data, size);
    uint32_t num_polygons;
    if (!decoder.get_varint32(&num_polygons)) {
        return false;
    }

    _polygons.clear();
    for (uint32_t i = 0; i < num_polygons; ++i) {
        std::unique_ptr<GeoPolygon> polygon = GeoPolygon::create_unique();
        polygon->_polygon.reset(new S2Polygon());
        if (!(polygon->_polygon->Decode(&decoder)) && polygon->_polygon->IsValid()) {
            return false;
        }
        _polygons.push_back(std::move(polygon));
    }
    return true;
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

bool GeoCircle::intersects(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        const S2Point& center = _cap->center();
        S1ChordAngle radius_angle = _cap->radius();
        // The radius unit of circle is initially in meters,
        // which needs to be converted back to meters when comparing
        double radius = S2Earth::RadiansToMeters(radius_angle.radians());
        return radius + TOLERANCE >= compute_distance_to_point(center, *point->point());
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        const S2Point& center = _cap->center();
        S1ChordAngle radius_angle = _cap->radius();
        double radius = S2Earth::RadiansToMeters(radius_angle.radians());

        double distance = compute_distance_to_line(center, line->polyline());
        return radius + TOLERANCE >= distance;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = assert_cast<const GeoPolygon*>(rhs);
        const S2Point& center = _cap->center();
        S1ChordAngle radius_angle = _cap->radius();
        if (is_point_in_polygon(center, polygon->polygon())) {
            return true;
        }
        double radius = S2Earth::RadiansToMeters(radius_angle.radians());
        for (int k = 0; k < polygon->polygon()->num_loops(); ++k) {
            const S2Loop* loop = polygon->polygon()->loop(k);
            for (int l = 0; l < loop->num_vertices(); ++l) {
                const S2Point& p1 = loop->vertex(l);
                const S2Point& p2 = loop->vertex((l + 1) % loop->num_vertices());
                double distance = compute_distance_to_line(center, p1, p2);

                if (radius + TOLERANCE >= distance) {
                    return true;
                }
            }
        }
        return false;
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        for (const auto& polygon : multi_polygon->polygons()) {
            if (this->intersects(polygon.get())) {
                return true;
            }
        }
        return false;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        S1ChordAngle radius_angle = _cap->radius();
        S1ChordAngle other_radius_angle = circle->circle()->radius();

        double radius1 = S2Earth::RadiansToMeters(radius_angle.radians());
        double radius2 = S2Earth::RadiansToMeters(other_radius_angle.radians());
        double distance = compute_distance_to_point(_cap->center(), circle->circle()->center());
        return radius1 + radius2 + TOLERANCE >= distance;
    }
    default:
        return false;
    }
}

bool GeoCircle::disjoint(const GeoShape* rhs) const {
    return !intersects(rhs);
}

bool GeoCircle::touches(const GeoShape* rhs) const {
    switch (rhs->type()) {
    case GEO_SHAPE_POINT: {
        const GeoPoint* point = assert_cast<const GeoPoint*>(rhs);
        const S2Point& center = _cap->center();
        S1ChordAngle radius_angle = _cap->radius();

        double radius = S2Earth::RadiansToMeters(radius_angle.radians());
        return std::abs(radius - compute_distance_to_point(center, *point->point())) < TOLERANCE;
    }
    case GEO_SHAPE_LINE_STRING: {
        const GeoLine* line = assert_cast<const GeoLine*>(rhs);
        const S2Point& center = _cap->center();
        S1ChordAngle radius_angle = _cap->radius();

        double radius = S2Earth::RadiansToMeters(radius_angle.radians());
        double distance = compute_distance_to_line(center, line->polyline());
        return std::abs(radius - distance) < TOLERANCE;
    }
    case GEO_SHAPE_POLYGON: {
        const GeoPolygon* polygon = assert_cast<const GeoPolygon*>(rhs);
        const S2Point& center = _cap->center();
        S1ChordAngle radius_angle = _cap->radius();
        if (is_point_in_polygon(center, polygon->polygon()) ||
            polygon->polygon_touch_point(polygon->polygon(), &center)) {
            return false;
        }

        double radius = S2Earth::RadiansToMeters(radius_angle.radians());
        for (int k = 0; k < polygon->polygon()->num_loops(); ++k) {
            const S2Loop* loop = polygon->polygon()->loop(k);
            for (int l = 0; l < loop->num_vertices(); ++l) {
                const S2Point& p1 = loop->vertex(l);
                const S2Point& p2 = loop->vertex((l + 1) % loop->num_vertices());
                double distance = compute_distance_to_line(center, p1, p2);

                if (std::abs(radius - distance) < TOLERANCE) {
                    return true;
                }
            }
        }

        return false;
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        const GeoMultiPolygon* multi_polygon = assert_cast<const GeoMultiPolygon*>(rhs);
        bool has_touches = false;
        for (const auto& polygon : multi_polygon->polygons()) {
            if (this->intersects(polygon.get())) {
                if (!this->touches(polygon.get())) {
                    return false;
                }
                has_touches = true;
            }
        }
        return has_touches;
    }
    case GEO_SHAPE_CIRCLE: {
        const GeoCircle* circle = assert_cast<const GeoCircle*>(rhs);
        S1ChordAngle radius_angle = _cap->radius();
        S1ChordAngle other_radius_angle = circle->circle()->radius();

        double radius1 = S2Earth::RadiansToMeters(radius_angle.radians());
        double radius2 = S2Earth::RadiansToMeters(other_radius_angle.radians());
        double distance = compute_distance_to_point(_cap->center(), circle->circle()->center());
        return std::abs(radius1 + radius2 - distance) < TOLERANCE;
    }
    default:
        return false;
    }
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

#include "common/compile_check_avoid_end.h"
} // namespace doris
