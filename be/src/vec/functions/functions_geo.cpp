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

#include "geo/geo_types.h"
#include "gutil/strings/substitute.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct StPoint {
    static constexpr auto NAME = "st_point";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        DCHECK_EQ(return_type->is_nullable(), true);
        auto column_x = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto column_y = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const auto size = column_x->size();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(null_type->get_nested_type()->create_column(),
                                     ColumnUInt8::create());

        GeoPoint point;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            if (column_x->is_null_at(row) || column_y->is_null_at(row)) {
                res->insert_data(nullptr, 0);
                continue;
            }

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

template<typename FunctionName>
struct StAsText {
    static constexpr auto NAME = FunctionName::NAME;
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments,size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        DCHECK_EQ(return_type->is_nullable(), true);
        auto input = block.get_by_position(arguments[0]).column;

        auto size = input->size();
        auto col = input->convert_to_full_column_if_const();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(
                null_type->get_nested_type()->create_column(), ColumnUInt8::create());

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            if (col->is_null_at(row)) {
                res->insert_data(nullptr, 0);
                continue;
            }
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
    static constexpr auto NAME = "st_x";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments,size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        DCHECK_EQ(return_type->is_nullable(), true);
        auto input = block.get_by_position(arguments[0]).column;

        auto size = input->size();
        auto col = input->convert_to_full_column_if_const();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(
                null_type->get_nested_type()->create_column(), ColumnUInt8::create());

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            if (col->is_null_at(row)) {
                res->insert_data(nullptr, 0);
                continue;
            }
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
    static constexpr auto NAME = "st_y";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments,size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        DCHECK_EQ(return_type->is_nullable(), true);
        auto input = block.get_by_position(arguments[0]).column;

        auto size = input->size();
        auto col = input->convert_to_full_column_if_const();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(
                null_type->get_nested_type()->create_column(), ColumnUInt8::create());

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            if (col->is_null_at(row)) {
                res->insert_data(nullptr, 0);
                continue;
            }
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
    static constexpr auto NAME = "st_distance_sphere";
    static const size_t NUM_ARGS = 4;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 4);
        auto return_type = block.get_data_type(result);
        DCHECK_EQ(return_type->is_nullable(), true);
        auto x_lng = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto x_lat = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto y_lng = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        auto y_lat = block.get_by_position(arguments[3]).column->convert_to_full_column_if_const();

        const auto size = x_lng->size();

        MutableColumnPtr res = nullptr;
        auto null_type = std::reinterpret_pointer_cast<const DataTypeNullable>(return_type);
        res = ColumnNullable::create(null_type->get_nested_type()->create_column(),
                                     ColumnUInt8::create());

        for (int row = 0; row < size; ++row) {
            if (x_lng->is_null_at(row) || x_lat->is_null_at(row) ||
                y_lng->is_null_at(row) || y_lat->is_null_at(row)) {
                res->insert_data(nullptr, 0);
                continue;
            }

            double distance;
            if (!GeoPoint::ComputeDistance(x_lng->operator[](row).get<Float64>(),
                                           x_lat->operator[](row).get<Float64>(),
                                           y_lng->operator[](row).get<Float64>(),
                                           y_lat->operator[](row).get<Float64>(),
                                           &distance)) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*) &distance), 0);
        }

        block.replace_by_position(result, std::move(res));
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
}

} // namespace doris::vectorized
