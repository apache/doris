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

#include "vec/functions/functions_geo.h"

#include "geo/geo_functions.h"
#include "geo/geo_types.h"
#include "gutil/strings/substitute.h"
#include "vec/columns/column_const.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct StPoint {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_point";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = remove_nullable(block.get_data_type(result));

        auto column_x =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto column_y =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const auto size = column_x->size();

        MutableColumnPtr res = nullptr;
        res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        GeoPoint point;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(column_x->operator[](row).get<Float64>(),
                                            column_y->operator[](row).get<Float64>());
            if (cur_res != GEO_PARSE_OK) {
                res->insert_data(nullptr, 0);
                continue;
            }

            buf.clear();
            point.encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct StAsTextName {
    static constexpr auto NAME = "st_astext";
};
struct StAsWktName {
    static constexpr auto NAME = "st_aswkt";
};

template <typename FunctionName>
struct StAsText {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = FunctionName::NAME;
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = remove_nullable(block.get_data_type(result));

        auto input = block.get_by_position(arguments[0]).column;

        auto size = input->size();
        auto col = input->convert_to_full_column_if_const();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto wkt = shape->as_wkt();
            res->insert_data(wkt.data(), wkt.size());
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

struct StX {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_x";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = remove_nullable(block.get_data_type(result));

        auto input = block.get_by_position(arguments[0]).column;

        auto size = input->size();
        auto col = input->convert_to_full_column_if_const();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            auto point_value = col->get_data_at(row);
            auto pt = point.decode_from(point_value.data, point_value.size);

            if (!pt) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto x_value = point.x();
            res->insert_data(const_cast<const char*>((char*)(&x_value)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

struct StY {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_y";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = remove_nullable(block.get_data_type(result));

        auto input = block.get_by_position(arguments[0]).column;

        auto size = input->size();
        auto col = input->convert_to_full_column_if_const();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            auto point_value = col->get_data_at(row);
            auto pt = point.decode_from(point_value.data, point_value.size);

            if (!pt) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto y_value = point.y();
            res->insert_data(const_cast<const char*>((char*)(&y_value)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

struct StDistanceSphere {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_distance_sphere";
    static const size_t NUM_ARGS = 4;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 4);
        auto return_type = remove_nullable(block.get_data_type(result));

        auto x_lng = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto x_lat = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto y_lng = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        auto y_lat = block.get_by_position(arguments[3]).column->convert_to_full_column_if_const();

        const auto size = x_lng->size();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        for (int row = 0; row < size; ++row) {
            double distance = 0;
            if (!GeoPoint::ComputeDistance(x_lng->operator[](row).get<Float64>(),
                                           x_lat->operator[](row).get<Float64>(),
                                           y_lng->operator[](row).get<Float64>(),
                                           y_lat->operator[](row).get<Float64>(), &distance)) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*)&distance), 0);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct StCircle {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = "st_circle";
    static const size_t NUM_ARGS = 3;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 3);
        auto return_type = remove_nullable(block.get_data_type(result));
        auto center_lng =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto center_lat =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto radius = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();

        const auto size = center_lng->size();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        StConstructState* state =
                (StConstructState*)context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
        if (state == nullptr) {
            GeoCircle circle;
            std::string buf;
            for (int row = 0; row < size; ++row) {
                auto lng_value = center_lng->get_float64(row);
                auto lat_value = center_lat->get_float64(row);
                auto radius_value = radius->get_float64(row);

                auto value = circle.init(lng_value, lat_value, radius_value);
                if (value != GEO_PARSE_OK) {
                    res->insert_data(nullptr, 0);
                    continue;
                }
                buf.clear();
                circle.encode_to(&buf);
                res->insert_data(buf.data(), buf.size());
            }
            block.replace_by_position(result, std::move(res));
        } else {
            if (state->is_null) {
                res->insert_data(nullptr, 0);
                block.replace_by_position(result, ColumnConst::create(std::move(res), size));
            } else {
                res->insert_data(state->encoded_buf.data(), state->encoded_buf.size());
                block.replace_by_position(result, ColumnConst::create(std::move(res), size));
            }
        }
        return Status::OK();
    }

    static Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        if (!context->is_arg_constant(0) || !context->is_arg_constant(1) ||
            !context->is_arg_constant(2)) {
            return Status::OK();
        }

        auto state = new StConstructState();
        DoubleVal* lng = reinterpret_cast<DoubleVal*>(context->get_constant_arg(0));
        DoubleVal* lat = reinterpret_cast<DoubleVal*>(context->get_constant_arg(1));
        DoubleVal* radius = reinterpret_cast<DoubleVal*>(context->get_constant_arg(2));
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
        context->set_function_state(scope, state);

        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }
        StConstructState* state =
                reinterpret_cast<StConstructState*>(context->get_function_state(scope));
        delete state;
        return Status::OK();
    }
};

struct StContains {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = "st_contains";
    static const size_t NUM_ARGS = 2;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = remove_nullable(block.get_data_type(result));
        auto shape1 = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto shape2 = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const auto size = shape1->size();
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        auto res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        StContainsState* state =
                (StContainsState*)context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
        if (state != nullptr && state->is_null) {
            res->insert_data(nullptr, 0);
            block.replace_by_position(result, ColumnConst::create(std::move(res), size));
            return Status::OK();
        }

        int i;
        std::vector<std::shared_ptr<GeoShape>> shapes = {nullptr, nullptr};
        for (int row = 0; row < size; ++row) {
            auto lhs_value = shape1->get_data_at(row);
            auto rhs_value = shape2->get_data_at(row);
            StringRef* strs[2] = {&lhs_value, &rhs_value};
            for (i = 0; i < 2; ++i) {
                if (state != nullptr && state->shapes[i] != nullptr) {
                    shapes[i] = state->shapes[i];
                } else {
                    shapes[i] = std::shared_ptr<GeoShape>(
                            GeoShape::from_encoded(strs[i]->data, strs[i]->size));
                    if (shapes[i] == nullptr) {
                        res->insert_data(nullptr, 0);
                        break;
                    }
                }
            }

            if (i == 2) {
                auto contains_value = shapes[0]->contains(shapes[1].get());
                res->insert_data(const_cast<const char*>((char*)&contains_value), 0);
            }
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        if (!context->is_arg_constant(0) && !context->is_arg_constant(1)) {
            return Status::OK();
        }

        auto contains_ctx = new StContainsState();
        for (int i = 0; !contains_ctx->is_null && i < 2; ++i) {
            if (context->is_arg_constant(i)) {
                StringVal* str = reinterpret_cast<StringVal*>(context->get_constant_arg(i));
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

        context->set_function_state(scope, contains_ctx);
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }
        StContainsState* state =
                reinterpret_cast<StContainsState*>(context->get_function_state(scope));
        delete state;
        return Status::OK();
    }
};

struct StGeometryFromText {
    static constexpr auto NAME = "st_geometryfromtext";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_ANY;
};

struct StGeomFromText {
    static constexpr auto NAME = "st_geomfromtext";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_ANY;
};

struct StLineFromText {
    static constexpr auto NAME = "st_linefromtext";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_LINE_STRING;
};

struct StLineStringFromText {
    static constexpr auto NAME = "st_linestringfromtext";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_LINE_STRING;
};

struct StPolygon {
    static constexpr auto NAME = "st_polygon";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_POLYGON;
};

struct StPolyFromText {
    static constexpr auto NAME = "st_polyfromtext";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_POLYGON;
};

struct StPolygonFromText {
    static constexpr auto NAME = "st_polygonfromtext";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_POLYGON;
};

template <typename Impl>
struct StGeoFromText {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = Impl::NAME;
    static const size_t NUM_ARGS = 1;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = remove_nullable(block.get_data_type(result));
        auto geo = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();

        const auto size = geo->size();
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        auto res = ColumnNullable::create(return_type->create_column(), ColumnUInt8::create());

        StConstructState* state =
                (StConstructState*)context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
        if (state == nullptr) {
            GeoParseStatus status;
            std::string buf;
            for (int row = 0; row < size; ++row) {
                auto value = geo->get_data_at(row);
                std::unique_ptr<GeoShape> shape(
                        GeoShape::from_wkt(value.data, value.size, &status));
                if (shape == nullptr || status != GEO_PARSE_OK ||
                    (Impl::shape_type != GEO_SHAPE_ANY && shape->type() != Impl::shape_type)) {
                    res->insert_data(nullptr, 0);
                    continue;
                }
                buf.clear();
                shape->encode_to(&buf);
                res->insert_data(buf.data(), buf.size());
            }
            block.replace_by_position(result, std::move(res));
        } else {
            if (state->is_null) {
                res->insert_data(nullptr, 0);
                block.replace_by_position(result, ColumnConst::create(std::move(res), size));
            } else {
                res->insert_data(state->encoded_buf.data(), state->encoded_buf.size());
                block.replace_by_position(result, ColumnConst::create(std::move(res), size));
            }
        }
        return Status::OK();
    }

    static Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        if (!context->is_arg_constant(0)) {
            return Status::OK();
        }

        auto state = new StConstructState();
        auto str_value = reinterpret_cast<StringVal*>(context->get_constant_arg(0));
        if (str_value->is_null) {
            state->is_null = true;
        } else {
            GeoParseStatus status;
            std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(
                    const_cast<const char*>((char*)str_value->ptr), str_value->len, &status));
            if (shape == nullptr ||
                (Impl::shape_type != GEO_SHAPE_ANY && shape->type() != Impl::shape_type)) {
                state->is_null = true;
            } else {
                shape->encode_to(&state->encoded_buf);
            }
        }

        context->set_function_state(scope, state);
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            StConstructState* state =
                    reinterpret_cast<StConstructState*>(context->get_function_state(scope));
            delete state;
        }
        return Status::OK();
    }
};

void register_geo_functions(SimpleFunctionFactory& factory) {
    factory.register_function<GeoFunction<StPoint>>();
    factory.register_function<GeoFunction<StAsText<StAsWktName>>>();
    factory.register_function<GeoFunction<StAsText<StAsTextName>>>();
    factory.register_function<GeoFunction<StX, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StY, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StDistanceSphere, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StContains, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StCircle>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeometryFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeomFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineStringFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygon>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygonFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolyFromText>>>();
}

} // namespace doris::vectorized
