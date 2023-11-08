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

#ifndef DORIS_GEOSHAPE_H
#define DORIS_GEOSHAPE_H

#pragma once

#include <stddef.h>

#include <memory>
#include <string>

#include "geo/geo_common.h"
#include "geo/wkt_parse_type.h"

class MutableS2ShapeIndex;

namespace doris {

class GeoShape {
public:
    virtual ~GeoShape() = default;

    virtual GeoShapeType type() const = 0;

    virtual bool is_valid() const { return false; }

    virtual bool is_closed() const { return false; }

    bool is_collection() const {
        if ((type() == GEO_SHAPE_MULTI_POINT || type() == GEO_SHAPE_MULTI_LINE_STRING ||
             type() == GEO_SHAPE_MULTI_POLYGON || type() == GEO_SHAPE_GEOMETRY_COLLECTION) &&
            !is_empty()) {
            return true;
        } else {
            return false;
        }
    }

    bool is_ring() const;

    bool is_empty() const { return empty; }

    void set_empty() { empty = true; }

    [[nodiscard]] virtual int get_dimension() const = 0;

    // decode from serialized data
    static GeoShape* from_encoded(const void* data, size_t& size);

    void encode_to(std::string* buf);

    bool decode_from(const void* data, size_t& data_size);

    // Returns the number of geometries in this collection
    // (or 1 if this is not a collection)
    virtual std::size_t get_num_geometries() const { return 1; }

    // \brief Returns a pointer to the nth Geometry in this collection
    // (or self if this is not a collection)
    virtual const GeoShape* get_geometries_n(std::size_t /*n*/) const { return this; }

    // Returns the number of point in this shape
    [[nodiscard]] virtual std::size_t get_num_point() const { return 0; }

    // try to construct a GeoShape from a WKT. If construct successfully, a GeoShape will
    // be returned, and the client should delete it when don't need it.
    // return nullptr if convert failed, and reason will be set in status
    static GeoShape* from_wkt(const char* data, size_t size, GeoParseStatus* status);

    virtual std::string as_wkt() const = 0;

    static GeoShape* from_wkb(const char* data, size_t size, GeoParseStatus* status);

    static std::string as_binary(GeoShape* rhs, int is_hex);
    static std::string geo_tohex(std::string binary);

    static GeoShape* from_geojson(const char* data, size_t size, GeoParseStatus* status);

    bool as_geojson(std::string& res);

    virtual bool contains(const GeoShape* rhs) const { return false; }

    bool intersects(GeoShape* shape) const;

    bool ComputeArea(double* angle, std::string square_unit);

    [[nodiscard]] double get_length() const;

    [[nodiscard]] double get_perimeter() const;

    [[nodiscard]] std::unique_ptr<GeoShape> get_centroid() const;

    [[nodiscard]] bool get_type_string(std::string& type_string) const;

    [[nodiscard]] virtual std::unique_ptr<GeoShape> boundary() const = 0;

    virtual bool add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const { return false; }

    //virtual MutableS2ShapeIndex* to_s2shape_index() const { return nullptr; };

    std::unique_ptr<GeoShape> find_closest_point(const GeoShape* shape) const;

    bool dwithin(const GeoShape* shape, double distance) const;

    std::unique_ptr<GeoShape> buffer(double buffer_radius, double num_seg_quarter_circle = 2,
                                     std::string end_cap = "ROUND",
                                     std::string side = "BOTH") const;

    std::unique_ptr<GeoShape> simplify();

protected:
    virtual void encode(std::string* buf, size_t& data_size) = 0;
    virtual bool decode(const void* data, size_t size) = 0;

    bool empty = false;
};

} // namespace doris

#endif //DORIS_GEOSHAPE_H
