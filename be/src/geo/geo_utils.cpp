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

#include "geo_utils.h"

#include <cmath>
#include <cstddef>
#include <numbers>

// use namespace to make them internal linkage
namespace {
constexpr double PI = std::numbers::pi_v<double>;
constexpr float PI_F = std::numbers::pi_v<float>;

constexpr float RAD_IN_DEG = static_cast<float>(PI / 180.0);
constexpr float RAD_IN_DEG_HALF = static_cast<float>(PI / 360.0);

constexpr size_t COS_LUT_SIZE = 1024;     // maxerr 0.00063%
constexpr float COS_LUT_SIZE_F = 1024.0F; // maxerr 0.00063%
constexpr size_t ASIN_SQRT_LUT_SIZE = 512;
constexpr size_t METRIC_LUT_SIZE = 1024;

/** Earth radius in meters using WGS84 authalic radius.
  * We use this value to be consistent with H3 library.
  */
constexpr float EARTH_RADIUS = 6371007.180918475F;
constexpr float EARTH_DIAMETER = 2 * EARTH_RADIUS;

float cos_lut[COS_LUT_SIZE + 1];             /// cos(x) table
float asin_sqrt_lut[ASIN_SQRT_LUT_SIZE + 1]; /// asin(sqrt(x)) * earth_diameter table

float sphere_metric_lut
        [METRIC_LUT_SIZE +
         1]; /// sphere metric, unitless: the distance in degrees for one degree across longitude depending on latitude
float sphere_metric_meters_lut
        [METRIC_LUT_SIZE +
         1]; /// sphere metric: the distance in meters for one degree across longitude depending on latitude
float wgs84_metric_meters_lut
        [2 *
         (METRIC_LUT_SIZE +
          1)]; /// ellipsoid metric: the distance in meters across one degree latitude/longitude depending on latitude

double sqr(double v) {
    return v * v;
}

float sqrf(float v) {
    return v * v;
}

__attribute__((no_sanitize("undefined"))) size_t float_to_index(float x) {
    return static_cast<size_t>(x);
}

float geodist_deg_diff(float f) {
    f = fabsf(f);
    if (f > 180) {
        f = 360 - f;
    }
    return f;
}

float geodist_fast_cos(float x) {
    float y = fabsf(x) * (COS_LUT_SIZE_F / PI_F / 2.0F);
    size_t i = float_to_index(y);
    y -= i;
    i &= (COS_LUT_SIZE - 1);
    return cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y;
}

float geodist_fast_sin(float x) {
    float y = fabsf(x) * (COS_LUT_SIZE_F / PI_F / 2.0F);
    size_t i = float_to_index(y);
    y -= i;
    i = (i - COS_LUT_SIZE / 4) &
        (COS_LUT_SIZE - 1); // cos(x - pi / 2) = sin(x), costable / 4 = pi / 2
    return cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y;
}

/// fast implementation of asin(sqrt(x))
/// max error in floats 0.00369%, in doubles 0.00072%
float geodist_fast_asin_sqrt(float x) {
    if (x < 0.122F) {
        // distance under 4546 km, Taylor error under 0.00072%
        float y = sqrtf(x);
        return y + x * y * 0.166666666666666F + x * x * y * 0.075F +
               x * x * x * y * 0.044642857142857F;
    }
    if (x < 0.948F) {
        // distance under 17083 km, 512-entry LUT error under 0.00072%
        x *= ASIN_SQRT_LUT_SIZE;
        size_t i = float_to_index(x);
        return asin_sqrt_lut[i] + (asin_sqrt_lut[i + 1] - asin_sqrt_lut[i]) * (x - i);
    }
    return asinf(sqrtf(x)); // distance over 17083 km, just compute exact
}
} // namespace

namespace doris::Geo {

void geodist_init() {
    for (size_t i = 0; i <= COS_LUT_SIZE; ++i) {
        cos_lut[i] = static_cast<float>(
                cos(2 * PI * i / COS_LUT_SIZE)); // [0, 2 * pi] -> [0, COS_LUT_SIZE]
    }

    for (size_t i = 0; i <= ASIN_SQRT_LUT_SIZE; ++i) {
        asin_sqrt_lut[i] = static_cast<float>(asin(sqrt(
                static_cast<double>(i) / ASIN_SQRT_LUT_SIZE))); // [0, 1] -> [0, ASIN_SQRT_LUT_SIZE]
    }

    for (size_t i = 0; i <= METRIC_LUT_SIZE; ++i) {
        double latitude =
                i * (PI / METRIC_LUT_SIZE) - PI * 0.5; // [-pi / 2, pi / 2] -> [0, METRIC_LUT_SIZE]

        /// Squared metric coefficients (for the distance in meters) on a tangent plane, for latitude and longitude (in degrees),
        /// depending on the latitude (in radians).

        /// https://github.com/mapbox/cheap-ruler/blob/master/index.js#L67
        wgs84_metric_meters_lut[i * 2] = static_cast<float>(
                sqr(111132.09 - 566.05 * cos(2 * latitude) + 1.20 * cos(4 * latitude)));
        wgs84_metric_meters_lut[i * 2 + 1] = static_cast<float>(sqr(
                111415.13 * cos(latitude) - 94.55 * cos(3 * latitude) + 0.12 * cos(5 * latitude)));

        sphere_metric_meters_lut[i] =
                static_cast<float>(sqr((EARTH_DIAMETER * PI / 360) * cos(latitude)));

        sphere_metric_lut[i] = static_cast<float>(sqr(cos(latitude)));
    }
}

float sphere_distance(float lon1deg, float lat1deg, float lon2deg, float lat2deg) {
    float lat_diff = geodist_deg_diff(lat1deg - lat2deg);
    float lon_diff = geodist_deg_diff(lon1deg - lon2deg);

    if (lon_diff < 13) {
        // points are close enough; use flat ellipsoid model
        // interpolate metric coefficients using latitudes midpoint

        /// Why comparing only difference in longitude?
        /// If longitudes are different enough, there is a big difference between great circle line and a line with constant latitude.
        ///  (Remember how a plane flies from Amsterdam to New York)
        /// But if longitude is close but latitude is different enough, there is no difference between meridian and great circle line.

        float latitude_midpoint = (lat1deg + lat2deg + 180) * METRIC_LUT_SIZE /
                                  360; // [-90, 90] degrees -> [0, METRIC_LUT_SIZE] indexes
        size_t latitude_midpoint_index = float_to_index(latitude_midpoint) & (METRIC_LUT_SIZE - 1);

        /// This is linear interpolation between two table items at index "latitude_midpoint_index" and "latitude_midpoint_index + 1".

        float k_lat {};
        float k_lon {};

        k_lat = sqrf(EARTH_DIAMETER * PI_F / 360.0F);

        k_lon = sphere_metric_meters_lut[latitude_midpoint_index] +
                (sphere_metric_meters_lut[latitude_midpoint_index + 1] -
                 sphere_metric_meters_lut[latitude_midpoint_index]) *
                        (latitude_midpoint - latitude_midpoint_index);

        /// Metric on a tangent plane: it differs from Euclidean metric only by scale of coordinates.
        return sqrtf(k_lat * lat_diff * lat_diff + k_lon * lon_diff * lon_diff);
    } else {
        // points too far away; use haversine
        float a = sqrf(geodist_fast_sin(lat_diff * RAD_IN_DEG_HALF)) +
                  geodist_fast_cos(lat1deg * RAD_IN_DEG) * geodist_fast_cos(lat2deg * RAD_IN_DEG) *
                          sqrf(geodist_fast_sin(lon_diff * RAD_IN_DEG_HALF));

        return EARTH_DIAMETER * geodist_fast_asin_sqrt(a);
    }
}
} // namespace doris::Geo