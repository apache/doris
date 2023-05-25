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

#ifndef DORIS_GEOCIRCLE_H
#define DORIS_GEOCIRCLE_H

#include "common/factory_creator.h"
#include "GeoShape.h"
#include "GeoPoint.h"

class S2Cap;

namespace doris {
    class GeoCircle : public GeoShape{
    ENABLE_FACTORY_CREATOR(GeoCircle);

    public:
        GeoCircle();
        ~GeoCircle() override;

        GeoParseStatus to_s2cap(double lng, double lat, double radius);

        GeoShapeType type() const override { return GEO_SHAPE_CIRCLE; }

        [[nodiscard]] int get_dimension() const override { return 2; }

        bool contains(const GeoShape* rhs) const override;
        std::string as_wkt() const override;

        double get_area() const;

        std::unique_ptr<GeoShape> boundary() const override;

    protected:
        void encode(std::string* buf, size_t& data_size) override;
        bool decode(const void* data, size_t size) override;

    private:
        std::unique_ptr<S2Cap> _cap;

    };
}



#endif //DORIS_GEOCIRCLE_H
