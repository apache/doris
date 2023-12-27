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

#include <stddef.h>

#include <memory>
#include <string>

#include "common/factory_creator.h"
#include "geo/geo_common.h"
#include "geo/wkt_parse_type.h"

class S2Polyline;
class S2Polygon;
class S2Cap;
class S2Loop;
template <typename T>
class Vector3;

using Vector3_d = Vector3<double>;

using S2Point = Vector3_d;

namespace doris {

class GeoShape {
public:
    virtual ~GeoShape() = default;

    virtual GeoShapeType type() const = 0;

    // decode from serialized data
    static std::unique_ptr<GeoShape> from_encoded(const void* data, size_t size);
    // try to construct a GeoShape from a WKT. If construct successfully, a GeoShape will
    // be returned, and the client should delete it when don't need it.
    // return nullptr if convert failed, and reason will be set in status
    static GeoShape* from_wkt(const char* data, size_t size, GeoParseStatus* status);

    static GeoShape* from_wkb(const char* data, size_t size, GeoParseStatus* status);

    void encode_to(std::string* buf);
    bool decode_from(const void* data, size_t size);

    virtual std::string as_wkt() const = 0;

    virtual bool contains(const GeoShape* rhs) const { return false; }
    virtual std::string to_string() const { return ""; }
    static std::string as_binary(GeoShape* rhs);

    static bool ComputeArea(GeoShape* rhs, double* angle, std::string square_unit);

protected:
    virtual void encode(std::string* buf) = 0;
    virtual bool decode(const void* data, size_t size) = 0;
};

class GeoPoint : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoPoint);

public:
    GeoPoint();
    ~GeoPoint() override;

    GeoParseStatus from_coord(double x, double y);
    GeoParseStatus from_coord(const GeoCoordinate& point);

    GeoCoordinateList to_coords() const;

    GeoShapeType type() const override { return GEO_SHAPE_POINT; }

    const S2Point* point() const { return _point.get(); }

    static bool ComputeDistance(double x_lng, double x_lat, double y_lng, double y_lat,
                                double* distance);

    static bool ComputeAngleSphere(double x_lng, double x_lat, double y_lng, double y_lat,
                                   double* angle);
    static bool ComputeAngle(GeoPoint* p1, GeoPoint* p2, GeoPoint* p3, double* angle);
    static bool ComputeAzimuth(GeoPoint* p1, GeoPoint* p2, double* angle);

    std::string to_string() const override;
    std::string as_wkt() const override;

    double x() const;
    double y() const;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Point> _point;
};

class GeoLine : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoLine);

public:
    GeoLine();
    ~GeoLine() override;

    GeoParseStatus from_coords(const GeoCoordinateList& list);

    GeoCoordinateList to_coords() const;

    GeoShapeType type() const override { return GEO_SHAPE_LINE_STRING; }
    const S2Polyline* polyline() const { return _polyline.get(); }

    std::string as_wkt() const override;

    int numPoint() const;
    S2Point* getPoint(int i) const;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Polyline> _polyline;
};

class GeoPolygon : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoPolygon);

public:
    GeoPolygon();
    ~GeoPolygon() override;

    GeoParseStatus from_coords(const GeoCoordinateListList& list);
    const std::unique_ptr<GeoCoordinateListList> to_coords() const;

    GeoShapeType type() const override { return GEO_SHAPE_POLYGON; }
    const S2Polygon* polygon() const { return _polygon.get(); }

    bool contains(const GeoShape* rhs) const override;
    std::string as_wkt() const override;

    int numLoops() const;
    double getArea() const;
    S2Loop* getLoop(int i) const;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Polygon> _polygon;
};

class GeoCircle : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoCircle);

public:
    GeoCircle();
    ~GeoCircle() override;

    GeoParseStatus init(double lng, double lat, double radius);

    GeoShapeType type() const override { return GEO_SHAPE_CIRCLE; }

    bool contains(const GeoShape* rhs) const override;
    std::string as_wkt() const override;

    double getArea() const;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Cap> _cap;
};

} // namespace doris
