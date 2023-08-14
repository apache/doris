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

#ifndef DORIS_GEOCOLLECTION_H
#define DORIS_GEOCOLLECTION_H

#include "GeoLineString.h"
#include "GeoPoint.h"
#include "GeoPolygon.h"
#include "GeoShape.h"
#include "common/factory_creator.h"

namespace doris {
class GeoCollection : public GeoShape {
    ENABLE_FACTORY_CREATOR(GeoCollection);

public:
    using const_iterator = std::vector<std::unique_ptr<GeoShape>>::const_iterator;

    using iterator = std::vector<std::unique_ptr<GeoShape>>::iterator;

    const_iterator begin() const { return geometries.begin(); };

    const_iterator end() const { return geometries.end(); };

    GeoCollection();
    ~GeoCollection() override;

    GeoShapeType type() const override { return GEO_SHAPE_GEOMETRY_COLLECTION; }

    bool is_valid() const override;

    bool is_closed() const override;

    void encode_to(std::string* buf) { GeoShape::encode_to(buf); };

    [[nodiscard]] int get_dimension() const override {
        if (get_num_geometries() == 0) {
            return -1;
        }

        int dim = get_geometries_n(0)->get_dimension();
        for (int i = 1; i < get_num_geometries(); i++) {
            if (dim < get_geometries_n(i)->get_dimension()) {
                dim = get_geometries_n(i)->get_dimension();
            }
        }

        return dim;
    }

    GeoParseStatus add_one_geometry(GeoShape* shape);

    // Returns the number of geometries in this collection
    std::size_t get_num_geometries() const override;

    // Returns the number of geometries in this collection
    GeoShape* get_geometries_n(std::size_t n) const override;

    std::string as_wkt() const override;

    bool contains(const GeoShape* rhs) const override;

    [[nodiscard]] std::size_t get_num_point() const override;

    std::unique_ptr<GeoShape> boundary() const override;

    std::unique_ptr<GeoCollection> to_homogenize();

    bool add_to_s2shape_index(MutableS2ShapeIndex& S2shape_index) const override;

protected:
    void encode(std::string* buf, size_t& data_size) override;
    bool decode(const void* data, size_t size) override;

    std::vector<std::unique_ptr<GeoShape>> geometries;
};

} // namespace doris

#endif //DORIS_GEOCOLLECTION_H
