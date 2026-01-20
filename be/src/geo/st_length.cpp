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

#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/util/units/length-units.h>

#include <cmath>

#include "geo/geo_types.h"

namespace doris {

double GeoPoint::Length() const {
    // Point has no length
    return 0.0;
}

double GeoLine::Length() const {
    // GeoLine is always valid with at least 2 vertices (guaranteed by constructor)
    double total_length = 0.0;
    for (int i = 0; i < _polyline->num_vertices() - 1; ++i) {
        const S2Point& p1 = _polyline->vertex(i);
        const S2Point& p2 = _polyline->vertex(i + 1);

        S2LatLng lat_lng1(p1);
        S2LatLng lat_lng2(p2);

        // Calculate distance in meters using S2Earth
        double distance = S2Earth::GetDistanceMeters(lat_lng1, lat_lng2);
        total_length += distance;
    }

    return total_length;
}

double GeoPolygon::Length() const {
    // GeoPolygon is always valid with at least one loop (guaranteed by constructor)
    double perimeter = 0.0;
    const S2Loop* outer_loop = _polygon->loop(0);

    for (int i = 0; i < outer_loop->num_vertices(); ++i) {
        const S2Point& p1 = outer_loop->vertex(i);
        const S2Point& p2 = outer_loop->vertex((i + 1) % outer_loop->num_vertices());

        S2LatLng lat_lng1(p1);
        S2LatLng lat_lng2(p2);

        // Calculate distance in meters using S2Earth
        double distance = S2Earth::GetDistanceMeters(lat_lng1, lat_lng2);
        perimeter += distance;
    }

    return perimeter;
}

double GeoMultiPolygon::Length() const {
    double total_length = 0.0;

    // Calculate the perimeter of all polygons
    for (const auto& polygon : _polygons) {
        total_length += polygon->Length();
    }

    return total_length;
}

double GeoCircle::Length() const {
    // GeoCircle is always valid (guaranteed by constructor)
    // Get the radius in meters
    double radius_meters = S2Earth::ToMeters(_cap->radius());

    // Calculate circumference: 2 * pi * r
    return 2.0 * M_PI * radius_meters;
}

} // namespace doris
