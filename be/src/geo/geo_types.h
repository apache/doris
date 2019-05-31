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

#include <memory>
#include <string>
#include <vector>

#include <s2/s2cap.h>
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#include <s2/s2polygon.h>

#include "geo/geo_common.h"
#include "geo/wkt_parse_type.h"

namespace doris {

class GeoShape {
public:
    virtual ~GeoShape() { }

    virtual GeoShapeType type() const = 0;

    // decode from serialized data
    static GeoShape* from_encoded(const void* data, size_t size);
    // try to construct a GeoShape from a WKT. If construct successfully, a GeoShape will
    // be returned, and the client should delete it when don't need it.
    // return nullptr if convert failed, and reason will be set in status
    static GeoShape* from_wkt(const char* data, size_t size, GeoParseStatus* status);

    void encode_to(std::string* buf);
    bool decode_from(const void* data, size_t size);

    virtual std::string as_wkt() const = 0;

    virtual bool contains(const GeoShape* rhs) const { return false; }
    virtual std::string to_string() const { return ""; };

protected:
    virtual void encode(std::string* buf) = 0;
    virtual bool decode(const void* data, size_t size) = 0;
};

class GeoPoint : public GeoShape {
public:
    GeoPoint() { }
    ~GeoPoint() override { }

    GeoParseStatus from_coord(double x, double y);
    GeoParseStatus from_coord(const GeoCoordinate& point);

    GeoShapeType type() const override { return GEO_SHAPE_POINT; }

    const S2Point& point() const { return _point; }

    std::string to_string() const override;
    std::string as_wkt() const override;

    double x() const;
    double y() const;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    S2Point _point;
};

class GeoLine : public GeoShape {
public:
    GeoLine() { }
    ~GeoLine() override { }

    GeoParseStatus from_coords(const GeoCoordinateList& list);

    GeoShapeType type() const override { return GEO_SHAPE_LINE_STRING; }
    const S2Polyline* polyline() const { return _polyline.get(); }

    std::string as_wkt() const override;
protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Polyline> _polyline;
};

class GeoPolygon : public GeoShape {
public:
    GeoPolygon() { }
    ~GeoPolygon() override { }

    GeoParseStatus from_coords(const GeoCoordinateListList& list);

    GeoShapeType type() const override { return GEO_SHAPE_POLYGON; }
    const S2Polygon* polygon() const { return _polygon.get(); }

    bool contains(const GeoShape* rhs) const override;
    std::string as_wkt() const override;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Polygon> _polygon;
};

class GeoCircle : public GeoShape {
public:
    GeoCircle() { }
    ~GeoCircle() { }

    GeoParseStatus init(double lng, double lat, double radius);

    GeoShapeType type() const override { return GEO_SHAPE_CIRCLE; }

    bool contains(const GeoShape* rhs) const override;
    std::string as_wkt() const override;

protected:
    void encode(std::string* buf) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Cap> _cap;
};

#if 0
class GeoMultiPoint : public GeoShape {
public:
    GeoPolygon();
    ~GeoPolygon() override;

    GeoShapeType type() const override { return GEO_SHAPE_POLYGON; }
    const std::vector<S2Point>& points() const { return _points; }

private:
    std::vector<S2Point> _points;
};

class GeoMultiLine : public GeoShape {
public:
    GeoMultiLine();
    ~GeoMultiLine() override;

    GeoShapeType type() const override { return GEO_SHAPE_MULTI_LINE_STRING; }
    const std::vector<S2Polyline*>& polylines() const { return _polylines; }

private:
    std::vector<S2Polyline> _polylines;
};

class GeoMultiPolygon : public GeoShape {
public:
    GeoMultiPolygon();
    ~GeoMultiPolygon() override;

    GeoShapeType type() const override { return GEO_SHAPE_MULTI_POLYGON; }

    const std::vector<S2Polygon>& polygons() const { return _polygons; }


    bool contains(const GeoShape* rhs) override;
private:
    std::vector<S2Polygon> _polygons;
};

#if 0
class GeoEnvelope : public GeoShape {
public:
};

class GeoCircle : public GeoShape {
public:
};
#endif

#endif

}

