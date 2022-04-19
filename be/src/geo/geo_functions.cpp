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

#include "geo/geo_functions.h"

#include <s2/s2debug.h>
#include <s2/s2earth.h>

#include "common/logging.h"
#include "geo/geo_types.h"

namespace doris {

void GeoFunctions::init() {
    // set s2debug to false to avoid crash
    FLAGS_s2debug = false;
}

DoubleVal GeoFunctions::st_distance_sphere(FunctionContext* ctx, const DoubleVal& x_lng,
                                           const DoubleVal& x_lat, const DoubleVal& y_lng,
                                           const DoubleVal& y_lat) {
    if (x_lng.is_null || x_lat.is_null || y_lng.is_null || y_lat.is_null) {
        return DoubleVal::null();
    }
    S2LatLng x = S2LatLng::FromDegrees(x_lat.val, x_lng.val);
    if (!x.is_valid()) {
        return DoubleVal::null();
    }
    S2LatLng y = S2LatLng::FromDegrees(y_lat.val, y_lng.val);
    if (!y.is_valid()) {
        return DoubleVal::null();
    }
    return DoubleVal(S2Earth::ToMeters(x.GetDistance(y)));
}

doris_udf::StringVal GeoFunctions::st_point(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DoubleVal& x,
                                            const doris_udf::DoubleVal& y) {
    if (x.is_null || y.is_null) {
        return StringVal::null();
    }
    GeoPoint point;
    auto res = point.from_coord(x.val, y.val);
    if (res != GEO_PARSE_OK) {
        return StringVal::null();
    }
    std::string buf;
    point.encode_to(&buf);
    StringVal result(ctx, buf.size());
    memcpy(result.ptr, buf.data(), buf.size());
    return result;
}

DoubleVal GeoFunctions::st_x(doris_udf::FunctionContext* ctx,
                             const doris_udf::StringVal& point_encoded) {
    if (point_encoded.is_null) {
        return DoubleVal::null();
    }
    GeoPoint point;
    auto res = point.decode_from(point_encoded.ptr, point_encoded.len);
    if (!res) {
        return DoubleVal::null();
    }
    return DoubleVal(point.x());
}

DoubleVal GeoFunctions::st_y(doris_udf::FunctionContext* ctx,
                             const doris_udf::StringVal& point_encoded) {
    if (point_encoded.is_null) {
        return DoubleVal::null();
    }
    GeoPoint point;
    auto res = point.decode_from(point_encoded.ptr, point_encoded.len);
    if (!res) {
        return DoubleVal::null();
    }
    return DoubleVal(point.y());
}

StringVal GeoFunctions::st_as_wkt(doris_udf::FunctionContext* ctx,
                                  const doris_udf::StringVal& shape_encoded) {
    if (shape_encoded.is_null) {
        return StringVal::null();
    }
    std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(shape_encoded.ptr, shape_encoded.len));
    if (shape == nullptr) {
        return StringVal::null();
    }
    auto wkt = shape->as_wkt();
    StringVal result(ctx, wkt.size());
    memcpy(result.ptr, wkt.data(), wkt.size());
    return result;
}

void GeoFunctions::st_from_wkt_close(FunctionContext* ctx,
                                     FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    StConstructState* state = reinterpret_cast<StConstructState*>(ctx->get_function_state(scope));
    delete state;
}

void GeoFunctions::st_from_wkt_prepare_common(FunctionContext* ctx,
                                              FunctionContext::FunctionStateScope scope,
                                              GeoShapeType shape_type) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    if (!ctx->is_arg_constant(0)) {
        return;
    }
    std::unique_ptr<StConstructState> state(new StConstructState());
    StringVal* str = reinterpret_cast<StringVal*>(ctx->get_constant_arg(0));
    if (str->is_null) {
        str->is_null = true;
    } else {
        GeoParseStatus status;
        std::unique_ptr<GeoShape> shape(
                GeoShape::from_wkt((const char*)str->ptr, str->len, &status));
        if (shape == nullptr || (shape_type != GEO_SHAPE_ANY && shape->type() != shape_type)) {
            state->is_null = true;
        } else {
            shape->encode_to(&state->encoded_buf);
        }
    }
    ctx->set_function_state(scope, state.release());
}

StringVal GeoFunctions::st_from_wkt_common(FunctionContext* ctx, const StringVal& wkt,
                                           GeoShapeType shape_type) {
    if (wkt.is_null) {
        return StringVal::null();
    }
    StConstructState* state =
            (StConstructState*)ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    if (state == nullptr) {
        GeoParseStatus status;
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt((const char*)wkt.ptr, wkt.len, &status));
        if (shape == nullptr || (shape_type != GEO_SHAPE_ANY && shape->type() != shape_type)) {
            return StringVal::null();
        }
        std::string buf;
        shape->encode_to(&buf);
        StringVal result(ctx, buf.size());
        memcpy(result.ptr, buf.data(), buf.size());
        return result;
    } else {
        if (state->is_null) {
            return StringVal::null();
        }
        StringVal result((uint8_t*)state->encoded_buf.data(), state->encoded_buf.size());
        return result;
    }
}

void GeoFunctions::st_circle_prepare(doris_udf::FunctionContext* ctx,
                                     doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    if (!ctx->is_arg_constant(0) || !ctx->is_arg_constant(1) || !ctx->is_arg_constant(2)) {
        return;
    }
    std::unique_ptr<StConstructState> state(new StConstructState());
    DoubleVal* lng = reinterpret_cast<DoubleVal*>(ctx->get_constant_arg(0));
    DoubleVal* lat = reinterpret_cast<DoubleVal*>(ctx->get_constant_arg(1));
    DoubleVal* radius = reinterpret_cast<DoubleVal*>(ctx->get_constant_arg(2));
    if (lng->is_null || lat->is_null || radius->is_null) {
        state->is_null = true;
    } else {
        std::unique_ptr<GeoCircle> circle(new GeoCircle());
        auto res = circle->init(lng->val, lat->val, radius->val);
        if (res != GEO_PARSE_OK) {
            state->is_null = true;
        } else {
            circle->encode_to(&state->encoded_buf);
        }
    }
    ctx->set_function_state(scope, state.release());
}

doris_udf::StringVal GeoFunctions::st_circle(FunctionContext* ctx, const DoubleVal& lng,
                                             const DoubleVal& lat, const DoubleVal& radius) {
    if (lng.is_null || lat.is_null || radius.is_null) {
        return StringVal::null();
    }
    StConstructState* state =
            (StConstructState*)ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    if (state == nullptr) {
        std::unique_ptr<GeoCircle> circle(new GeoCircle());
        auto res = circle->init(lng.val, lat.val, radius.val);
        if (res != GEO_PARSE_OK) {
            return StringVal::null();
        }
        std::string buf;
        circle->encode_to(&buf);
        StringVal result(ctx, buf.size());
        memcpy(result.ptr, buf.data(), buf.size());
        return result;
    } else {
        if (state->is_null) {
            return StringVal::null();
        }
        StringVal result((uint8_t*)state->encoded_buf.data(), state->encoded_buf.size());
        return result;
    }
}

void GeoFunctions::st_contains_prepare(doris_udf::FunctionContext* ctx,
                                       doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    if (!ctx->is_arg_constant(0) && !ctx->is_arg_constant(1)) {
        return;
    }
    std::unique_ptr<StContainsState> contains_ctx(new StContainsState());
    for (int i = 0; !contains_ctx->is_null && i < 2; ++i) {
        if (ctx->is_arg_constant(i)) {
            StringVal* str = reinterpret_cast<StringVal*>(ctx->get_constant_arg(i));
            if (str->is_null) {
                contains_ctx->is_null = true;
            } else {
                contains_ctx->shapes[i] =
                        std::shared_ptr<GeoShape>(GeoShape::from_encoded(str->ptr, str->len));
                if (contains_ctx->shapes[i] == nullptr) {
                    contains_ctx->is_null = true;
                }
            }
        }
    }
    ctx->set_function_state(scope, contains_ctx.release());
}

void GeoFunctions::st_contains_close(doris_udf::FunctionContext* ctx,
                                     doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    StContainsState* contains_ctx =
            reinterpret_cast<StContainsState*>(ctx->get_function_state(scope));
    delete contains_ctx;
}

doris_udf::BooleanVal GeoFunctions::st_contains(doris_udf::FunctionContext* ctx,
                                                const doris_udf::StringVal& lhs,
                                                const doris_udf::StringVal& rhs) {
    if (lhs.is_null || rhs.is_null) {
        return BooleanVal::null();
    }
    const StContainsState* state = reinterpret_cast<StContainsState*>(
            ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state != nullptr && state->is_null) {
        return BooleanVal::null();
    }
    std::vector<std::shared_ptr<GeoShape>> shapes = {nullptr, nullptr};
    const StringVal* strs[2] = {&lhs, &rhs};
    for (int i = 0; i < 2; ++i) {
        if (state != nullptr && state->shapes[i] != nullptr) {
            shapes[i] = state->shapes[i];
        } else {
            shapes[i] =
                    std::shared_ptr<GeoShape>(GeoShape::from_encoded(strs[i]->ptr, strs[i]->len));
            if (shapes[i] == nullptr) {
                return BooleanVal::null();
            }
        }
    }

    return shapes[0]->contains(shapes[1].get());
}

} // namespace doris
