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

#include "GeoCircle.h"

#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/util/coding/coder.h>

namespace doris {

GeoCircle::GeoCircle() = default;
GeoCircle::~GeoCircle() = default;

GeoParseStatus GeoCircle::to_s2cap(double lng, double lat, double radius_meter) {
    S2Point center;
    auto status = GeoPoint::to_s2point(lng, lat, &center);
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

std::string GeoCircle::as_wkt() const {
    std::stringstream ss;
    ss << "CIRCLE ((";
    GeoPoint::print_s2point(ss, _cap->center());
    ss << "), " << S2Earth::ToMeters(_cap->radius()) << ")";
    return ss.str();
}

double GeoCircle::get_area() const {
    return _cap->GetArea();
}

std::unique_ptr<GeoShape> GeoCircle::boundary() const {
    return nullptr;
}

void GeoCircle::encode(std::string* buf, size_t& data_size) {
    Encoder encoder;
    _cap->Encode(&encoder);
    data_size = encoder.length();
    buf->append(encoder.base(), encoder.length());
}

bool GeoCircle::decode(const void* data, size_t size) {
    Decoder decoder(data, size);
    _cap.reset(new S2Cap());
    return _cap->Decode(&decoder) && _cap->is_valid();
}

} // namespace doris
