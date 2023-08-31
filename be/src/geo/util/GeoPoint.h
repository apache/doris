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

#ifndef DORIS_GEOPOINT_H
#define DORIS_GEOPOINT_H

#include "GeoShape.h"
#include "common/factory_creator.h"
#include "geo/wkt_parse_type.h"

template <typename T>
class Vector3;
class S2Shape;
using S2Point = Vector3<double>;

namespace doris {

class GeoPoint : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoPoint);

public:
    GeoPoint();
    ~GeoPoint() override;

    static void print_s2point(std::ostream& os, const S2Point& point);

    static GeoParseStatus to_s2point(double lng, double lat, S2Point* point);
    static GeoParseStatus to_s2point(const GeoCoordinate& coord, S2Point* point);

    GeoParseStatus from_coord(double x, double y);
    GeoParseStatus from_coord(const GeoCoordinate& point);

    GeoParseStatus from_s2point(S2Point* point);

    [[nodiscard]] GeoCoordinates to_coords() const;

    [[nodiscard]] std::unique_ptr<GeoCoordinate> to_coord() const;

    [[nodiscard]] GeoShapeType type() const override { return GEO_SHAPE_POINT; }

    [[nodiscard]] bool is_valid() const override;

    [[nodiscard]] bool is_closed() const override { return !is_empty(); }

    [[nodiscard]] int get_dimension() const override { return 0; }

    [[nodiscard]] const S2Point* point() const { return _point.get(); }

    [[nodiscard]] std::unique_ptr<S2Shape> get_s2shape() const;

    bool contains(const GeoShape* rhs) const override;

    static bool ComputeDistance(double x_lng, double x_lat, double y_lng, double y_lat,
                                double* distance);

    static bool ComputeAngleSphere(double x_lng, double x_lat, double y_lng, double y_lat,
                                   double* angle);
    static bool ComputeAngle(GeoPoint* p1, GeoPoint* p2, GeoPoint* p3, double* angle);
    static bool ComputeAzimuth(GeoPoint* p1, GeoPoint* p2, double* angle);

    [[nodiscard]] std::string as_wkt() const override;

    [[nodiscard]] std::size_t get_num_point() const override { return 1; }

    [[nodiscard]] std::unique_ptr<GeoShape> boundary() const override;

    bool add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const override;

    //void add_to_s2point_index(S2PointIndex<int>& S2point_index, int i);

    [[nodiscard]] double x() const;
    [[nodiscard]] double y() const;

protected:
    void encode(std::string* buf, size_t& data_size) override;
    bool decode(const void* data, size_t size) override;

private:
    std::unique_ptr<S2Point> _point;
};

} // namespace doris

#endif //DORIS_GEOPOINT_H
