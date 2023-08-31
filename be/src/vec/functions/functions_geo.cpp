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

#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <string>
#include <utility>

#include "geo/geo_common.h"
#include "geo/util/GeoCircle.h"
#include "geo/util/GeoLineString.h"
#include "geo/util/GeoPoint.h"
#include "geo/util/GeoPolygon.h"
#include "geo/util/GeoShape.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct StPoint {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_point";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);

        auto column_x =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto column_y =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const auto size = column_x->size();

        MutableColumnPtr res = return_type->create_column();

        GeoPoint point;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(column_x->operator[](row).get<Float64>(),
                                            column_y->operator[](row).get<Float64>());

            if (cur_res != GEO_PARSE_OK) {
                return Status::InvalidArgument(to_string(cur_res));
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
        auto return_type = block.get_data_type(result);

        auto& input = block.get_by_position(arguments[0]).column;

        auto size = input->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input->get_data_at(row);
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
        auto return_type = block.get_data_type(result);

        auto& input = block.get_by_position(arguments[0]).column;

        auto size = input->size();

        MutableColumnPtr res = return_type->create_column();

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
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
        auto return_type = block.get_data_type(result);

        auto& input = block.get_by_position(arguments[0]).column;

        auto size = input->size();

        MutableColumnPtr res = return_type->create_column();

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
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
        auto return_type = block.get_data_type(result);

        auto x_lng = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto x_lat = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto y_lng = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        auto y_lat = block.get_by_position(arguments[3]).column->convert_to_full_column_if_const();

        const auto size = x_lng->size();

        MutableColumnPtr res = return_type->create_column();

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

struct StAngleSphere {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_angle_sphere";
    static const size_t NUM_ARGS = 4;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 4);
        auto return_type = block.get_data_type(result);

        auto x_lng = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto x_lat = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto y_lng = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        auto y_lat = block.get_by_position(arguments[3]).column->convert_to_full_column_if_const();

        const auto size = x_lng->size();

        MutableColumnPtr res = return_type->create_column();

        for (int row = 0; row < size; ++row) {
            double angle = 0;
            if (!GeoPoint::ComputeAngleSphere(x_lng->operator[](row).get<Float64>(),
                                              x_lat->operator[](row).get<Float64>(),
                                              y_lng->operator[](row).get<Float64>(),
                                              y_lat->operator[](row).get<Float64>(), &angle)) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*)&angle), 0);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct StAngle {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_angle";
    static const size_t NUM_ARGS = 3;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 3);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto p1 = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto p2 = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto p3 = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        const auto size = p1->size();

        GeoPoint point1;
        GeoPoint point2;
        GeoPoint point3;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = p1->get_data_at(row);
            auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);
            if (!pt1) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto shape_value2 = p2->get_data_at(row);
            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);
            if (!pt2) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto shape_value3 = p3->get_data_at(row);
            auto pt3 = point3.decode_from(shape_value3.data, shape_value3.size);
            if (!pt3) {
                res->insert_data(nullptr, 0);
                continue;
            }

            double angle = 0;
            if (!GeoPoint::ComputeAngle(&point1, &point2, &point3, &angle)) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*)&angle), 0);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct StAzimuth {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_azimuth";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto p1 = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto p2 = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto size = p1->size();

        GeoPoint point1;
        GeoPoint point2;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = p1->get_data_at(row);
            auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);
            if (!pt1) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto shape_value2 = p2->get_data_at(row);
            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);
            if (!pt2) {
                res->insert_data(nullptr, 0);
                continue;
            }

            double angle = 0;
            if (!GeoPoint::ComputeAzimuth(&point1, &point2, &angle)) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*)&angle), 0);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct StAreaSquareMeters {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_area_square_meters";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));
            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            double area = 0;
            if (!shape->ComputeArea(&area, "square_meters")) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*)&area), 0);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct StAreaSquareKm {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_area_square_km";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));
            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            double area = 0;
            if (!shape->ComputeArea(&area, "square_km")) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(const_cast<const char*>((char*)&area), 0);
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
        auto return_type = block.get_data_type(result);
        auto center_lng =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto center_lat =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto radius = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();

        const auto size = center_lng->size();

        MutableColumnPtr res = return_type->create_column();

        GeoCircle circle;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto lng_value = center_lng->get_float64(row);
            auto lat_value = center_lat->get_float64(row);
            auto radius_value = radius->get_float64(row);

            auto value = circle.to_s2cap(lng_value, lat_value, radius_value);
            if (value != GEO_PARSE_OK) {
                res->insert_data(nullptr, 0);
                continue;
            }
            buf.clear();
            circle.encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
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
        auto return_type = block.get_data_type(result);
        auto shape1 = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto shape2 = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const auto size = shape1->size();
        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> lhs_shape;
        std::unique_ptr<GeoShape> rhs_shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = shape1->get_data_at(0);
            lhs_shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));
            if (lhs_shape == nullptr) {
                res->insert_data(nullptr, 0);
                block.replace_by_position(result, std::move(res));
                return Status::OK();
            }

            auto rhs_value = shape2->get_data_at(row);
            rhs_shape.reset(GeoShape::from_encoded(rhs_value.data, rhs_value.size));
            if (rhs_shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto contains_value = lhs_shape->contains(rhs_shape.get());
            res->insert_data(const_cast<const char*>((char*)&contains_value), 0);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
};

struct StWithin {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = "st_within";
    static const size_t NUM_ARGS = 2;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        auto shape1 = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto shape2 = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const auto size = shape1->size();
        MutableColumnPtr res = return_type->create_column();

        int i;
        std::vector<std::shared_ptr<GeoShape>> shapes = {nullptr, nullptr};
        for (int row = 0; row < size; ++row) {
            auto lhs_value = shape1->get_data_at(row);
            auto rhs_value = shape2->get_data_at(row);
            StringRef* strs[2] = {&lhs_value, &rhs_value};
            for (i = 0; i < 2; ++i) {
                shapes[i] = std::shared_ptr<GeoShape>(
                        GeoShape::from_encoded(strs[i]->data, strs[i]->size));
                if (shapes[i] == nullptr) {
                    res->insert_data(nullptr, 0);
                    break;
                }
            }

            if (i == 2) {
                auto contains_value = shapes[1]->contains(shapes[0].get());
                res->insert_data(const_cast<const char*>((char*)&contains_value), 0);
            }
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
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
        auto return_type = block.get_data_type(result);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        MutableColumnPtr res = return_type->create_column();

        GeoParseStatus status;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(value.data, value.size, &status));

            if (status != GEO_PARSE_OK) {
                return Status::InvalidArgument(to_string(status));
            }

            if (shape == nullptr ||
                (Impl::shape_type != GEO_SHAPE_ANY && shape->type() != Impl::shape_type)) {
                res->insert_data(nullptr, 0);
                continue;
            }
            buf.clear();
            shape->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
};

struct StGeometryFromWKB {
    static constexpr auto NAME = "st_geometryfromwkb";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_ANY;
};

struct StGeomFromWKB {
    static constexpr auto NAME = "st_geomfromwkb";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_ANY;
};

template <typename Impl>
struct StGeoFromWkb {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = Impl::NAME;
    static const size_t NUM_ARGS = 1;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        MutableColumnPtr res = return_type->create_column();

        GeoParseStatus status;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            if (value.size == 0) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::unique_ptr<GeoShape> shape(GeoShape::from_wkb(value.data, value.size, &status));

            if (status != GEO_PARSE_OK) {
                return Status::InvalidArgument(to_string(status));
            }

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            buf.clear();
            shape->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
};

struct StAsBinary {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = "st_asbinary";
    static const size_t NUM_ARGS = 1;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));
            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::string binary = GeoShape::as_binary(shape.get(), 1);
            if (binary.empty()) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(binary.data(), binary.size());
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
};

struct StAsGeoJson {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = "st_asgeojson";
    static const size_t NUM_ARGS = 1;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));
            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::string geojson;
            if (shape->as_geojson(geojson) && geojson.empty()) {
                res->insert_data(nullptr, 0);
                continue;
            }
            res->insert_data(geojson.data(), geojson.size());
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
};

struct StGeometryFromGeoJson {
    static constexpr auto NAME = "st_geometryfromgeojson";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_ANY;
};

struct StGeomFromGeoJson {
    static constexpr auto NAME = "st_geomfromgeojson";
    static constexpr GeoShapeType shape_type = GEO_SHAPE_ANY;
};
template <typename Impl>
struct StGeoFromGeoJson {
    static constexpr auto NEED_CONTEXT = true;
    static constexpr auto NAME = Impl::NAME;
    static const size_t NUM_ARGS = 1;
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        MutableColumnPtr res = return_type->create_column();

        GeoParseStatus status;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            if (value.size == 0) {
                res->insert_data(nullptr, 0);
                continue;
            }
            std::unique_ptr<GeoShape> shape(
                    GeoShape::from_geojson(value.data, value.size, &status));

            if (status != GEO_PARSE_OK) {
                return Status::InvalidArgument(to_string(status));
            }

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            buf.clear();
            shape->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
};

struct StPointN {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_pointn";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto line_arg =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto index_arg =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto size = line_arg->size();

        GeoLineString line;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = line_arg->get_data_at(row);
            auto pt = line.decode_from(shape_value1.data, shape_value1.size);
            if (!pt) {
                //here error
                res->insert_data(nullptr, 0);
                continue;
            }

            GeoPoint point;
            std::string buf;

            int index = index_arg->get_int(row);
            if (index > 0 && index <= line.num_point()) {
                index--;
            } else if (index < 0 && -index <= line.num_point()) {
                index = line.num_point() + index + 1;
            } else {
                return Status::InvalidArgument("The vertex index " + std::to_string(index) +
                                               " is out of bounds; input LINESTRING has " +
                                               std::to_string(line.num_point()) + " points.");
            }

            if (point.from_s2point(line.get_point(index)) != GEO_PARSE_OK) {
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

//ST_STARTPOINT
struct StStartPoint {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_startpoint";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto line_arg =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = line_arg->size();

        GeoLineString line;
        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = line_arg->get_data_at(row);
            auto pt = line.decode_from(shape_value1.data, shape_value1.size);
            if (!pt) {
                res->insert_data(nullptr, 0);
                continue;
            }

            GeoPoint point;

            if (point.from_s2point(line.get_point(0)) != GEO_PARSE_OK) {
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

//ST_ENDPOINT
struct StEndPoint {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_endpoint";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto line_arg =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = line_arg->size();

        GeoLineString line;
        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = line_arg->get_data_at(row);
            auto pt = line.decode_from(shape_value1.data, shape_value1.size);
            if (!pt) {
                res->insert_data(nullptr, 0);
                continue;
            }

            GeoPoint point;

            if (point.from_s2point(line.get_point(line.num_point() - 1)) != GEO_PARSE_OK) {
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

//ST_DIMENSION
struct StDimension {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_dimension";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto dimension = shape->get_dimension();
            res->insert_data(const_cast<const char*>((char*)(&dimension)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_ISEMPTY
struct StIsEmpty {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_isempty";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto is_empty = shape->is_empty();
            res->insert_data(const_cast<const char*>((char*)(&is_empty)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_LENGTH
struct StLength {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_length";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto length = shape->get_length();
            res->insert_data(const_cast<const char*>((char*)(&length)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_ISCLOSED
struct StIsClosed {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_isclosed";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto is_closed = shape->is_closed();
            res->insert_data(const_cast<const char*>((char*)(&is_closed)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_ISCOLLECTION
struct StIsCollection {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_iscollection";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto is_collection = shape->is_collection();
            res->insert_data(const_cast<const char*>((char*)(&is_collection)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_ISRING
struct StIsRing {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_isring";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto is_ring = shape->is_ring();
            res->insert_data(const_cast<const char*>((char*)(&is_ring)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_NUMGEOMETRIES
struct StNumGeometries {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_numgeometries";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto num_geometries = shape->get_num_geometries();
            res->insert_data(const_cast<const char*>((char*)(&num_geometries)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_NumPoints
struct StNumPoints {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_numpoints";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            auto num_point = shape->get_num_point();
            res->insert_data(const_cast<const char*>((char*)(&num_point)), 0);
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_GEOMETRYTYPE
struct StGeometryType {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_geometrytype";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        std::string type;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            type.clear();
            if (shape->get_type_string(type)) {
                res->insert_data(type.data(), type.size());
            } else {
                res->insert_data(nullptr, 0);
                continue;
            }
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_CENTROID
struct StCentroid {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_centroid";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto input_shape = block.get_by_position(arguments[0]).column;

        auto size = input_shape->size();

        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value = input_shape->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::unique_ptr<GeoShape> shape_res = shape->get_centroid();
            //std::unique_ptr<GeoShape> shape_res = shape->boundary();

            if (shape_res == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            buf.clear();
            shape_res->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

//ST_LINELOCATEPOINT
struct StLineLocatePoint {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_linelocatepoint";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto line_arg =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto point_arg =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto size = line_arg->size();

        GeoLineString line;
        GeoPoint point;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = line_arg->get_data_at(row);
            auto pt1 = line.decode_from(shape_value1.data, shape_value1.size);
            if (!pt1) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto shape_value2 = point_arg->get_data_at(row);
            auto pt2 = point.decode_from(shape_value2.data, shape_value2.size);
            if (!pt2) {
                res->insert_data(nullptr, 0);
                continue;
            }

            double percentage = line.line_locate_point(&point);

            res->insert_data(const_cast<const char*>((char*)&percentage), 0);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

//ST_BOUNDARY
struct StBoundary {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_boundary";
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();

        std::unique_ptr<GeoShape> shape;
        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));
            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::unique_ptr<GeoShape> shape_res = shape->boundary();

            if (shape_res == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            buf.clear();
            shape_res->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

//ST_CLOSESTPOINT
struct StClosestPoint {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_closestpoint";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto shape_arg1 =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto shape_arg2 =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto size = shape_arg1->size();

        std::unique_ptr<GeoShape> shape1;
        std::unique_ptr<GeoShape> shape2;

        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = shape_arg1->get_data_at(row);
            shape1.reset(GeoShape::from_encoded(shape_value1.data, shape_value1.size));
            if (shape1 == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto shape_value2 = shape_arg2->get_data_at(row);
            shape2.reset(GeoShape::from_encoded(shape_value2.data, shape_value2.size));
            if (shape2 == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::unique_ptr<GeoShape> res_point = shape1->find_closest_point(shape2.get());

            if (res_point == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            buf.clear();
            res_point->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

//ST_INTERSECTS
struct StIntersects {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_intersects";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto shape_arg1 =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto shape_arg2 =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto size = shape_arg1->size();

        std::unique_ptr<GeoShape> shape1;
        std::unique_ptr<GeoShape> shape2;

        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = shape_arg1->get_data_at(row);
            shape1.reset(GeoShape::from_encoded(shape_value1.data, shape_value1.size));
            if (shape1 == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto shape_value2 = shape_arg2->get_data_at(row);
            shape2.reset(GeoShape::from_encoded(shape_value2.data, shape_value2.size));
            if (shape2 == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto intersects_value = shape1->intersects(shape2.get());

            res->insert_data(const_cast<const char*>((char*)&intersects_value), 0);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

//ST_DWITHIN
struct StDwithin {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_dwithin";
    static const size_t NUM_ARGS = 3;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 3);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        auto shape_arg1 =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto shape_arg2 =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto distance =
                block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        const auto size = shape_arg1->size();

        std::unique_ptr<GeoShape> shape1;
        std::unique_ptr<GeoShape> shape2;

        std::string buf;

        for (int row = 0; row < size; ++row) {
            auto shape_value1 = shape_arg1->get_data_at(row);
            shape1.reset(GeoShape::from_encoded(shape_value1.data, shape_value1.size));
            if (shape1 == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto shape_value2 = shape_arg2->get_data_at(row);
            shape2.reset(GeoShape::from_encoded(shape_value2.data, shape_value2.size));
            if (shape2 == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            auto dwithin_value =
                    shape1->dwithin(shape2.get(), distance->operator[](row).get<Float64>());

            res->insert_data(const_cast<const char*>((char*)&dwithin_value), 0);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

//ST_BUFFER
struct StBuffer {
    static constexpr auto NEED_CONTEXT = false;
    static constexpr auto NAME = "st_buffer";
    static const size_t NUM_ARGS = 2;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);

        auto shape_input =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto buffer_radius =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        auto size = shape_input->size();
        MutableColumnPtr res = return_type->create_column();

        std::unique_ptr<GeoShape> shape;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto shape_value = shape_input->get_data_at(row);
            shape.reset(GeoShape::from_encoded(shape_value.data, shape_value.size));

            if (shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }
            std::unique_ptr<GeoShape> res_shape;
            res_shape.reset(shape->buffer(buffer_radius->operator[](row).get<Float64>()).release());

            if (res_shape == nullptr) {
                res->insert_data(nullptr, 0);
                continue;
            }

            buf.clear();
            res_shape->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

void register_function_geo(SimpleFunctionFactory& factory) {
    factory.register_function<GeoFunction<StPoint>>();
    factory.register_function<GeoFunction<StAsText<StAsWktName>, DataTypeString>>();
    factory.register_function<GeoFunction<StAsText<StAsTextName>, DataTypeString>>();
    factory.register_function<GeoFunction<StX, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StY, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StDistanceSphere, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAngleSphere, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAngle, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAzimuth, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StContains, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StWithin, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StCircle>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeometryFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeomFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineStringFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygon>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygonFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolyFromText>>>();
    factory.register_function<GeoFunction<StAreaSquareMeters, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAreaSquareKm, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StGeoFromWkb<StGeometryFromWKB>>>();
    factory.register_function<GeoFunction<StGeoFromWkb<StGeomFromWKB>>>();
    factory.register_function<GeoFunction<StAsBinary, DataTypeString>>();
    factory.register_function<GeoFunction<StGeoFromGeoJson<StGeometryFromGeoJson>>>();
    factory.register_function<GeoFunction<StGeoFromGeoJson<StGeomFromGeoJson>>>();
    factory.register_function<GeoFunction<StAsGeoJson, DataTypeString>>();
    factory.register_function<GeoFunction<StPointN>>();
    factory.register_function<GeoFunction<StStartPoint>>();
    factory.register_function<GeoFunction<StEndPoint>>();
    factory.register_function<GeoFunction<StLineLocatePoint, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StBoundary>>();
    factory.register_function<GeoFunction<StClosestPoint>>();
    factory.register_function<GeoFunction<StIntersects, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StBuffer>>();
    factory.register_function<GeoFunction<StDwithin, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StDimension, DataTypeInt32>>();
    factory.register_function<GeoFunction<StIsEmpty, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StLength, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StIsClosed, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StIsCollection, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StIsRing, DataTypeUInt8>>();
    factory.register_function<GeoFunction<StNumGeometries, DataTypeInt64>>();
    factory.register_function<GeoFunction<StNumPoints, DataTypeInt64>>();
    factory.register_function<GeoFunction<StGeometryType, DataTypeString>>();
    factory.register_function<GeoFunction<StCentroid>>();
}

} // namespace doris::vectorized
