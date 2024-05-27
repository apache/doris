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
#include <utility>

#include "geo/geo_common.h"
#include "geo/geo_types.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
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

        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        const auto size = std::max(left_column->size(), right_column->size());

        MutableColumnPtr res = return_type->create_column();

        GeoPoint point;
        std::string buf;
        if (left_const) {
            const_vector(left_column, right_column, res, size, point, buf);
        } else if (right_const) {
            vector_const(left_column, right_column, res, size, point, buf);
        } else {
            vector_vector(left_column, right_column, res, size, point, buf);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static void loop_do(GeoParseStatus& cur_res, MutableColumnPtr& res, GeoPoint& point,
                        std::string& buf) {
        if (cur_res != GEO_PARSE_OK) {
            res->insert_data(nullptr, 0);
            return;
        }

        buf.clear();
        point.encode_to(&buf);
        res->insert_data(buf.data(), buf.size());
    }

    static void const_vector(const ColumnPtr& left_column, const ColumnPtr& right_column,
                             MutableColumnPtr& res, const size_t size, GeoPoint& point,
                             std::string& buf) {
        double x = left_column->operator[](0).get<Float64>();
        for (int row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(x, right_column->operator[](row).get<Float64>());
            loop_do(cur_res, res, point, buf);
        }
    }

    static void vector_const(const ColumnPtr& left_column, const ColumnPtr& right_column,
                             MutableColumnPtr& res, const size_t size, GeoPoint& point,
                             std::string& buf) {
        double y = right_column->operator[](0).get<Float64>();
        for (int row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(right_column->operator[](row).get<Float64>(), y);
            loop_do(cur_res, res, point, buf);
        }
    }

    static void vector_vector(const ColumnPtr& left_column, const ColumnPtr& right_column,
                              MutableColumnPtr& res, const size_t size, GeoPoint& point,
                              std::string& buf) {
        for (int row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(left_column->operator[](row).get<Float64>(),
                                            right_column->operator[](row).get<Float64>());
            loop_do(cur_res, res, point, buf);
        }
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
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
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
                res->insert_default();
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
                res->insert_default();
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
                res->insert_default();
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
                res->insert_default();
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
                res->insert_default();
                continue;
            }

            auto shape_value2 = p2->get_data_at(row);
            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);
            if (!pt2) {
                res->insert_default();
                continue;
            }
            auto shape_value3 = p3->get_data_at(row);
            auto pt3 = point3.decode_from(shape_value3.data, shape_value3.size);
            if (!pt3) {
                res->insert_default();
                continue;
            }

            double angle = 0;
            if (!GeoPoint::ComputeAngle(&point1, &point2, &point3, &angle)) {
                res->insert_default();
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

        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        const auto size = std::max(left_column->size(), right_column->size());

        GeoPoint point1;
        GeoPoint point2;
        if (left_const) {
            const_vector(left_column, right_column, res, size, point1, point2);
        } else if (right_const) {
            vector_const(left_column, right_column, res, size, point1, point2);
        } else {
            vector_vector(left_column, right_column, res, size, point1, point2);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static void loop_do(bool& pt1, bool& pt2, GeoPoint& point1, GeoPoint& point2,
                        MutableColumnPtr& res) {
        if (!(pt1 && pt2)) {
            res->insert_default();
            return;
        }

        double angle = 0;
        if (!GeoPoint::ComputeAzimuth(&point1, &point2, &angle)) {
            res->insert_default();
            return;
        }
        res->insert_data(const_cast<const char*>((char*)&angle), 0);
    }

    static void const_vector(const ColumnPtr& left_column, const ColumnPtr& right_column,
                             MutableColumnPtr& res, size_t size, GeoPoint& point1,
                             GeoPoint& point2) {
        auto shape_value1 = left_column->get_data_at(0);
        auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);
        for (int row = 0; row < size; ++row) {
            auto shape_value2 = right_column->get_data_at(row);
            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);

            loop_do(pt1, pt2, point1, point2, res);
        }
    }

    static void vector_const(const ColumnPtr& left_column, const ColumnPtr& right_column,
                             MutableColumnPtr& res, size_t size, GeoPoint& point1,
                             GeoPoint& point2) {
        auto shape_value2 = right_column->get_data_at(0);
        auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);
        for (int row = 0; row < size; ++row) {
            auto shape_value1 = left_column->get_data_at(row);
            auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);

            loop_do(pt1, pt2, point1, point2, res);
        }
    }

    static void vector_vector(const ColumnPtr& left_column, const ColumnPtr& right_column,
                              MutableColumnPtr& res, size_t size, GeoPoint& point1,
                              GeoPoint& point2) {
        for (int row = 0; row < size; ++row) {
            auto shape_value1 = left_column->get_data_at(row);
            auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);
            auto shape_value2 = right_column->get_data_at(row);
            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);

            loop_do(pt1, pt2, point1, point2, res);
        }
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
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (!shape) {
                res->insert_default();
                continue;
            }

            double area = 0;
            if (!GeoShape::ComputeArea(shape.get(), &area, "square_meters")) {
                res->insert_default();
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
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (!shape) {
                res->insert_default();
                continue;
            }

            double area = 0;
            if (!GeoShape::ComputeArea(shape.get(), &area, "square_km")) {
                res->insert_default();
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
        const auto* center_lng_ptr = assert_cast<const ColumnFloat64*>(center_lng.get());
        auto center_lat =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto* center_lat_ptr = assert_cast<const ColumnFloat64*>(center_lat.get());
        auto radius = block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        const auto* radius_ptr = assert_cast<const ColumnFloat64*>(radius.get());

        const auto size = center_lng->size();

        MutableColumnPtr res = return_type->create_column();

        GeoCircle circle;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto lng_value = center_lng_ptr->get_element(row);
            auto lat_value = center_lat_ptr->get_element(row);
            auto radius_value = radius_ptr->get_element(row);

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
        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        const auto size = std::max(left_column->size(), right_column->size());

        MutableColumnPtr res = return_type->create_column();

        if (left_const) {
            const_vector(left_column, right_column, res, size);
        } else if (right_const) {
            vector_const(left_column, right_column, res, size);
        } else {
            vector_vector(left_column, right_column, res, size);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

    static void loop_do(StringRef& lhs_value, StringRef& rhs_value,
                        std::vector<std::shared_ptr<GeoShape>>& shapes, int& i,
                        MutableColumnPtr& res) {
        StringRef* strs[2] = {&lhs_value, &rhs_value};
        for (i = 0; i < 2; ++i) {
            shapes[i] =
                    std::shared_ptr<GeoShape>(GeoShape::from_encoded(strs[i]->data, strs[i]->size));
            if (shapes[i] == nullptr) {
                res->insert_default();
                break;
            }
        }

        if (i == 2) {
            auto contains_value = shapes[0]->contains(shapes[1].get());
            res->insert_data(const_cast<const char*>((char*)&contains_value), 0);
        }
    }

    static void const_vector(const ColumnPtr& left_column, const ColumnPtr& right_column,
                             MutableColumnPtr& res, const size_t size) {
        int i;
        auto lhs_value = left_column->get_data_at(0);
        std::vector<std::shared_ptr<GeoShape>> shapes = {nullptr, nullptr};
        for (int row = 0; row < size; ++row) {
            auto rhs_value = right_column->get_data_at(row);
            loop_do(lhs_value, rhs_value, shapes, i, res);
        }
    }

    static void vector_const(const ColumnPtr& left_column, const ColumnPtr& right_column,
                             MutableColumnPtr& res, const size_t size) {
        int i;
        auto rhs_value = right_column->get_data_at(0);
        std::vector<std::shared_ptr<GeoShape>> shapes = {nullptr, nullptr};
        for (int row = 0; row < size; ++row) {
            auto lhs_value = left_column->get_data_at(row);
            loop_do(lhs_value, rhs_value, shapes, i, res);
        }
    }

    static void vector_vector(const ColumnPtr& left_column, const ColumnPtr& right_column,
                              MutableColumnPtr& res, const size_t size) {
        int i;
        std::vector<std::shared_ptr<GeoShape>> shapes = {nullptr, nullptr};
        for (int row = 0; row < size; ++row) {
            auto lhs_value = left_column->get_data_at(row);
            auto rhs_value = right_column->get_data_at(row);
            loop_do(lhs_value, rhs_value, shapes, i, res);
        }
    }

    static Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    static Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }
}; // namespace doris::vectorized

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
            std::unique_ptr<GeoShape> shape(GeoShape::from_wkb(value.data, value.size, &status));
            if (shape == nullptr || status != GEO_PARSE_OK) {
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

        auto col = block.get_by_position(arguments[0]).column;
        const auto size = col->size();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (!shape) {
                res->insert_data(nullptr, 0);
                continue;
            }

            std::string binary = GeoShape::as_binary(shape.get());
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

void register_function_geo(SimpleFunctionFactory& factory) {
    factory.register_function<GeoFunction<StPoint>>();
    factory.register_function<GeoFunction<StAsText<StAsWktName>>>();
    factory.register_function<GeoFunction<StAsText<StAsTextName>>>();
    factory.register_function<GeoFunction<StX, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StY, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StDistanceSphere, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAngleSphere, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAngle, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StAzimuth, DataTypeFloat64>>();
    factory.register_function<GeoFunction<StContains, DataTypeUInt8>>();
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
    factory.register_function<GeoFunction<StAsBinary>>();
}

} // namespace doris::vectorized
