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

#ifndef DORIS_GEOPOLYGON_H
#define DORIS_GEOPOLYGON_H

#include "common/factory_creator.h"
#include "GeoShape.h"
#include "GeoPoint.h"
#include "GeoLineString.h"
#include "geo/wkt_parse_type.h"

class S2Polygon;
class S2Loop;

namespace doris {
    class GeoPolygon : public GeoShape{
    ENABLE_FACTORY_CREATOR(GeoPolygon);

    public:
        GeoPolygon();
        ~GeoPolygon() override;

        bool is_valid() const override;

        bool is_closed() const override ;

        static GeoParseStatus to_s2loop(const GeoCoordinates& coords, std::unique_ptr<S2Loop>* loop);

        GeoParseStatus from_coords(const GeoCoordinateLists& coords_list);
        const std::unique_ptr<GeoCoordinateLists> to_coords() const;

        GeoParseStatus from_s2loop(std::vector<std::unique_ptr<S2Loop>>& loops);

        GeoParseStatus from_s2polygon(S2Polygon* s2polygon);

        GeoShapeType type() const override { return GEO_SHAPE_POLYGON; }

        [[nodiscard]] int get_dimension() const override { return 2; }

        double get_perimeter() const;

        const S2Polygon* polygon() const { return _polygon.get(); }

        std::unique_ptr<S2Shape> get_s2shape() const;

        bool contains(const GeoShape* rhs) const override;

        static void print_coords(std::ostream& os, const GeoPolygon& polygon);
        std::string as_wkt() const override;

        int num_loops() const;
        double get_area() const;
        S2Loop* get_loop(int i) const;

        [[nodiscard]] std::size_t get_num_point() const override;

        std::unique_ptr<GeoShape> boundary() const override;

        bool add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const override;

    protected:
        void encode(std::string* buf, size_t& data_size) override;
        bool decode(const void* data, size_t size) override;

    private:
        std::unique_ptr<S2Polygon> _polygon;
    };

}// namespace doris



#endif //DORIS_GEOPOLYGON_H
