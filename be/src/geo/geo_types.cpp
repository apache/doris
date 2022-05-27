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

#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/util/coding/coder.h>
#include <s2/util/units/length-units.h>
#include <stdio.h>

#include <iomanip>
#include <sstream>

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
    return abs(lng) <= 180 && abs(lat) <= 90;
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
        if (i != 0 && !loops[0]->Contains(loops[i].get())) {
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

GeoShape* GeoShape::from_encoded(const void* ptr, size_t size) {
    if (size < 2 || ((const char*)ptr)[0] != 0X00) {
        return nullptr;
    }
    std::unique_ptr<GeoShape> shape;
    switch (((const char*)ptr)[1]) {
    case GEO_SHAPE_POINT: {
        shape.reset(new GeoPoint());
        break;
    }
    case GEO_SHAPE_LINE_STRING: {
        shape.reset(new GeoLine());
        break;
    }
    case GEO_SHAPE_POLYGON: {
        shape.reset(new GeoPolygon());
        break;
    }
    case GEO_SHAPE_CIRCLE: {
        shape.reset(new GeoCircle());
        break;
    }
    default:
        return nullptr;
    }
    auto res = shape->decode((const char*)ptr + 2, size - 2);
    if (!res) {
        return nullptr;
    }
    return shape.release();
}

GeoParseStatus GeoPoint::from_coord(double x, double y) {
    return to_s2point(x, y, _point.get());
}

GeoParseStatus GeoPoint::from_coord(const GeoCoordinate& coord) {
    return to_s2point(coord, _point.get());
}

std::string GeoPoint::to_string() const {
    return as_wkt();
}

void GeoPoint::encode(std::string* buf) {
    buf->append((const char*)_point.get(), sizeof(*_point));
}

bool GeoPoint::decode(const void* data, size_t size) {
    if (size < sizeof(*_point)) {
        return false;
    }
    memcpy(_point.get(), data, size);
    return true;
}

double GeoPoint::x() const {
    return S2LatLng(*_point).lng().degrees();
}

double GeoPoint::y() const {
    return S2LatLng(*_point).lat().degrees();
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
    *distance = S2Earth::ToMeters(x.GetDistance(y));
    return true;
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
    return _polygon->Decode(&decoder);
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
        return _polygon->Contains(other->polygon());
    }
    default:
        return false;
    }
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
    return _cap->Decode(&decoder);
}

std::string GeoCircle::as_wkt() const {
    std::stringstream ss;
    ss << "CIRCLE ((";
    print_s2point(ss, _cap->center());
    ss << "), " << S2Earth::ToMeters(_cap->radius()) << ")";
    return ss.str();
}

} // namespace doris
