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

#ifndef DORIS_GEOMULTIPOLYGON_H
#define DORIS_GEOMULTIPOLYGON_H

#include "common/factory_creator.h"
//#include "GeoMultiLineString.h"
#include "GeoCollection.h"

namespace doris {

class GeoMultiPolygon : public GeoCollection {
    ENABLE_FACTORY_CREATOR(GeoMultiPolygon);

public:
    GeoMultiPolygon();
    ~GeoMultiPolygon() override;

    GeoShapeType type() const override { return GEO_SHAPE_MULTI_POLYGON; }

    [[nodiscard]] int get_dimension() const override { return 2; }

    GeoParseStatus from_coords(const GeoCoordinateListCollections& coord_list_collections);

    std::unique_ptr<GeoCoordinateListCollections> to_coords() const;

    // Returns the number of geometries in this collection
    std::size_t get_num_polygon() const;

    GeoPolygon* get_polygon_n(std::size_t n) const;

    std::string as_wkt() const override;

    bool contains(const GeoShape* shape) const override;

    std::unique_ptr<GeoShape> boundary() const override;
};

} // namespace doris

#endif //DORIS_GEOMULTIPOLYGON_H
