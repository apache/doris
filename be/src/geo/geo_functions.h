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

#pragma once

#include "geo/geo_common.h"
#include "geo/geo_types.h"
#include "udf/udf.h"

namespace doris {

class GeoFunctions {
public:
    static void init();

    // compute distance between two points in earth sphere
    static DoubleVal st_distance_sphere(FunctionContext* ctx, const DoubleVal& x_lng,
                                        const DoubleVal& x_lat, const DoubleVal& y_lng,
                                        const DoubleVal& y_lat);

    // point
    static doris_udf::StringVal st_point(doris_udf::FunctionContext* ctx,
                                         const doris_udf::DoubleVal& x,
                                         const doris_udf::DoubleVal& y);

    static doris_udf::DoubleVal st_x(doris_udf::FunctionContext* ctx,
                                     const doris_udf::StringVal& point);
    static doris_udf::DoubleVal st_y(doris_udf::FunctionContext* ctx,
                                     const doris_udf::StringVal& point);

    // to wkt
    static doris_udf::StringVal st_as_wkt(doris_udf::FunctionContext* ctx,
                                          const doris_udf::StringVal& shape);
    // from wkt
    static void st_from_wkt_prepare_common(doris_udf::FunctionContext*,
                                           doris_udf::FunctionContext::FunctionStateScope,
                                           GeoShapeType shape_type);
    static void st_from_wkt_close(doris_udf::FunctionContext*,
                                  doris_udf::FunctionContext::FunctionStateScope);
    static doris_udf::StringVal st_from_wkt_common(doris_udf::FunctionContext* ctx,
                                                   const doris_udf::StringVal& wkt,
                                                   GeoShapeType shape_type);

    static void st_from_wkt_prepare(doris_udf::FunctionContext* ctx,
                                    doris_udf::FunctionContext::FunctionStateScope scope)
            __attribute__((used)) {
        st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_ANY);
    }
    static doris_udf::StringVal st_from_wkt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& wkt) __attribute__((used)) {
        return st_from_wkt_common(ctx, wkt, GEO_SHAPE_ANY);
    }

    // for line
    static void st_line_prepare(doris_udf::FunctionContext* ctx,
                                doris_udf::FunctionContext::FunctionStateScope scope)
            __attribute__((used)) {
        st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_LINE_STRING);
    }
    static doris_udf::StringVal st_line(doris_udf::FunctionContext* ctx,
                                        const doris_udf::StringVal& wkt) __attribute__((used)) {
        return st_from_wkt_common(ctx, wkt, GEO_SHAPE_LINE_STRING);
    }

    // for polygon
    static void st_polygon_prepare(doris_udf::FunctionContext* ctx,
                                   doris_udf::FunctionContext::FunctionStateScope scope)
            __attribute__((used)) {
        st_from_wkt_prepare_common(ctx, scope, GEO_SHAPE_POLYGON);
    }
    static doris_udf::StringVal st_polygon(doris_udf::FunctionContext* ctx,
                                           const doris_udf::StringVal& wkt) __attribute__((used)) {
        return st_from_wkt_common(ctx, wkt, GEO_SHAPE_POLYGON);
    }

    // for circle
    static doris_udf::StringVal st_circle(doris_udf::FunctionContext* ctx,
                                          const doris_udf::DoubleVal& center_lng,
                                          const doris_udf::DoubleVal& center_lat,
                                          const doris_udf::DoubleVal& radius_meter);
    static void st_circle_prepare(doris_udf::FunctionContext*,
                                  doris_udf::FunctionContext::FunctionStateScope);

    // Returns true if and only if no points of the second geometry
    // lie in the exterior of the first geometry, and at least one
    // point of the interior of the first geometry lies in the
    // interior of the second geometry.
    static doris_udf::BooleanVal st_contains(doris_udf::FunctionContext* ctx,
                                             const doris_udf::StringVal& lhs,
                                             const doris_udf::StringVal& rhs);
    static void st_contains_prepare(doris_udf::FunctionContext*,
                                    doris_udf::FunctionContext::FunctionStateScope);
    static void st_contains_close(doris_udf::FunctionContext*,
                                  doris_udf::FunctionContext::FunctionStateScope);
};

struct StConstructState {
    StConstructState() : is_null(false) {}
    ~StConstructState() {}

    bool is_null;
    std::string encoded_buf;
};

struct StContainsState {
    StContainsState() : is_null(false), shapes {nullptr, nullptr} {}
    ~StContainsState() {}
    bool is_null;
    std::vector<std::shared_ptr<GeoShape>> shapes;
};

} // namespace doris
