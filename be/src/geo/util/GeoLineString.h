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

#ifndef DORIS_GEOLINESTRING_H
#define DORIS_GEOLINESTRING_H

#include "GeoPoint.h"
#include "GeoShape.h"
#include "common/factory_creator.h"
#include "geo/wkt_parse_type.h"

class S2Polyline;

namespace doris {

class GeoLineString : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoLineString);

public:
    GeoLineString();
    ~GeoLineString() override;

    bool is_valid() const override;

    bool is_closed() const override;

    static GeoParseStatus to_s2polyline(const GeoCoordinates& coords,
                                        std::unique_ptr<S2Polyline>* polyline);

    GeoParseStatus from_coords(const GeoCoordinates& coords);

    GeoCoordinates to_coords() const;

    std::unique_ptr<GeoCoordinates> to_coords_ptr() const;

    GeoShapeType type() const override { return GEO_SHAPE_LINE_STRING; }

    [[nodiscard]] int get_dimension() const override { return 1; }

    [[nodiscard]] double length() const;

    const S2Polyline* polyline() const { return _polyline.get(); }

    std::unique_ptr<S2Shape> get_s2shape() const;

    std::string as_wkt() const override;

    int num_point() const;
    S2Point* get_point(int i) const;

    double line_locate_point(GeoPoint* point);

    [[nodiscard]] std::size_t get_num_point() const override;

    std::unique_ptr<GeoShape> boundary() const override;

    bool add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const override;

protected:
    void encode(std::string* buf, size_t& data_size) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Polyline> _polyline;
};

} // namespace doris

#endif //DORIS_GEOLINESTRING_H
