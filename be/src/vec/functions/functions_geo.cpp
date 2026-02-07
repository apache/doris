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
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_execute_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct StPoint {
    static constexpr auto NAME = "st_point";
    static const size_t NUM_ARGS = 2;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        auto left_column_ptr = block.get_by_position(arguments[0]).column;
        auto right_column_ptr = block.get_by_position(arguments[1]).column;
        const auto size = std::max(left_column_ptr->size(), right_column_ptr->size());
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        GeoPoint point;
        std::string buf;
        auto func = [&](size_t i, double left_val, double right_val) ALWAYS_INLINE {
            auto geo_status = point.from_coord(left_val, right_val);
            if (geo_status != GEO_PARSE_OK) {
                null_map_data[i] = 1;
                res->insert_default();
                return;
            }
            buf.clear();
            point.encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        };

        ExecuteColumn::execute_binary_compile_time_only_const<TYPE_DOUBLE, TYPE_DOUBLE>(
                left_column_ptr, right_column_ptr, func);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
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
    static constexpr auto NAME = FunctionName::NAME;
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto& input = block.get_by_position(arguments[0]).column;

        auto size = input->size();

        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input->get_data_at(row);
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (shape == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto wkt = shape->as_wkt();
            res->insert_data(wkt.data(), wkt.size());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));

        return Status::OK();
    }
};

struct StX {
    static constexpr auto NAME = "st_x";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto& input = block.get_by_position(arguments[0]).column;

        auto size = input->size();

        auto res = ColumnFloat64::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        res->reserve(size);

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            auto pt = point.decode_from(point_value.data, point_value.size);

            if (!pt) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto x_value = point.x();
            res->insert_value(x_value);
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));

        return Status::OK();
    }
};

struct StY {
    static constexpr auto NAME = "st_y";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto& input = block.get_by_position(arguments[0]).column;

        auto size = input->size();

        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        GeoPoint point;
        for (int row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            auto pt = point.decode_from(point_value.data, point_value.size);

            if (!pt) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto y_value = point.y();
            res->insert_value(y_value);
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));

        return Status::OK();
    }
};

struct StDistanceSphere {
    static constexpr auto NAME = "st_distance_sphere";
    static const size_t NUM_ARGS = 4;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 4);
        auto return_type = block.get_data_type(result);

        auto x_lng = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[0]).column);
        auto x_lat = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[1]).column);
        auto y_lng = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[2]).column);
        auto y_lat = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[3]).column);

        const auto size = x_lng.size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        for (int row = 0; row < size; ++row) {
            double distance = 0;
            if (!GeoPoint::ComputeDistance(x_lng.value_at(row), x_lat.value_at(row),
                                           y_lng.value_at(row), y_lat.value_at(row), &distance)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            res->insert_value(distance);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StAngleSphere {
    static constexpr auto NAME = "st_angle_sphere";
    static const size_t NUM_ARGS = 4;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 4);
        auto return_type = block.get_data_type(result);

        auto x_lng = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[0]).column);
        auto x_lat = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[1]).column);
        auto y_lng = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[2]).column);
        auto y_lat = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[3]).column);

        const auto size = x_lng.size();

        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        for (int row = 0; row < size; ++row) {
            double angle = 0;
            if (!GeoPoint::ComputeAngleSphere(x_lng.value_at(row), x_lat.value_at(row),
                                              y_lng.value_at(row), y_lat.value_at(row), &angle)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            res->insert_value(angle);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StAngle {
    static constexpr auto NAME = "st_angle";
    static const size_t NUM_ARGS = 3;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 3);
        auto return_type = block.get_data_type(result);

        auto p1 = block.get_by_position(arguments[0]).column;
        auto p2 = block.get_by_position(arguments[1]).column;
        auto p3 = block.get_by_position(arguments[2]).column;

        const auto size = p1->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        GeoPoint point1;
        GeoPoint point2;
        GeoPoint point3;

        auto func = [&](size_t i, StringRef shape_value1, StringRef shape_value2,
                        StringRef shape_value3) ALWAYS_INLINE {
            auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);
            if (!pt1) {
                null_map_data[i] = 1;
                res->insert_default();
                return;
            }

            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);
            if (!pt2) {
                null_map_data[i] = 1;
                res->insert_default();
                return;
            }
            auto pt3 = point3.decode_from(shape_value3.data, shape_value3.size);
            if (!pt3) {
                null_map_data[i] = 1;
                res->insert_default();
                return;
            }

            double angle = 0;
            if (!GeoPoint::ComputeAngle(&point1, &point2, &point3, &angle)) {
                null_map_data[i] = 1;
                res->insert_default();
                return;
            }
            res->insert_value(angle);
        };

        ExecuteColumn::execute_ternary_compile_time_only_const<TYPE_STRING, TYPE_STRING,
                                                               TYPE_STRING>(p1, p2, p3, func);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StAzimuth {
    static constexpr auto NAME = "st_azimuth";
    static const size_t NUM_ARGS = 2;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);

        auto left_column = block.get_by_position(arguments[0]).column;
        auto right_column = block.get_by_position(arguments[1]).column;

        const auto size = std::max(left_column->size(), right_column->size());
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        GeoPoint point1;
        GeoPoint point2;

        auto func = [&](size_t i, StringRef shape_value1, StringRef shape_value2) ALWAYS_INLINE {
            auto pt1 = point1.decode_from(shape_value1.data, shape_value1.size);
            auto pt2 = point2.decode_from(shape_value2.data, shape_value2.size);

            loop_do(pt1, pt2, point1, point2, res, null_map_data, i);
        };
        ExecuteColumn::execute_binary_compile_time_only_const<TYPE_STRING, TYPE_STRING>(
                left_column, right_column, func);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }

    static void loop_do(bool& pt1, bool& pt2, GeoPoint& point1, GeoPoint& point2,
                        ColumnFloat64::MutablePtr& res, NullMap& null_map, size_t row) {
        if (!(pt1 && pt2)) {
            null_map[row] = 1;
            res->insert_default();
            return;
        }

        double angle = 0;
        if (!GeoPoint::ComputeAzimuth(&point1, &point2, &angle)) {
            null_map[row] = 1;
            res->insert_default();
            return;
        }
        res->insert_value(angle);
    }
};

struct StAreaSquareMeters {
    static constexpr auto NAME = "st_area_square_meters";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double area = 0;
            if (!GeoShape::ComputeArea(shape.get(), &area, "square_meters")) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            res->insert_value(area);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StAreaSquareKm {
    static constexpr auto NAME = "st_area_square_km";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto size = col->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double area = 0;
            if (!GeoShape::ComputeArea(shape.get(), &area, "square_km")) {
                null_map_data[row] = 1;
                res->insert_default();
                ;
                continue;
            }
            res->insert_value(area);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StCircle {
    static constexpr auto NAME = "st_circle";
    static const size_t NUM_ARGS = 3;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 3);
        auto return_type = block.get_data_type(result);
        auto center_lng_col = block.get_by_position(arguments[0]).column;
        auto center_lat_col = block.get_by_position(arguments[1]).column;
        auto radius_col = block.get_by_position(arguments[2]).column;

        const auto size = center_lng_col->size();

        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        GeoCircle circle;
        std::string buf;

        auto func = [&](size_t i, double lng_value, double lat_value, double radius_value)
                            ALWAYS_INLINE {
                                auto value = circle.init(lng_value, lat_value, radius_value);
                                if (value != GEO_PARSE_OK) {
                                    null_map_data[i] = 1;
                                    res->insert_default();
                                    return;
                                }
                                buf.clear();
                                circle.encode_to(&buf);
                                res->insert_data(buf.data(), buf.size());
                            };

        ExecuteColumn::execute_ternary_compile_time_only_const<TYPE_DOUBLE, TYPE_DOUBLE,
                                                               TYPE_DOUBLE>(
                center_lng_col, center_lat_col, radius_col, func);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

template <typename Func>
struct StRelationFunction {
    static constexpr auto NAME = Func::NAME;
    static const size_t NUM_ARGS = 2;
    using Type = DataTypeUInt8;

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        auto left_column = block.get_by_position(arguments[0]).column;
        auto right_column = block.get_by_position(arguments[1]).column;

        const auto size = left_column->size();

        auto res = ColumnUInt8::create(size, 0);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        auto func = [&](size_t i, StringRef lhs_value, StringRef rhs_value) ALWAYS_INLINE {
            std::unique_ptr<GeoShape> shape1(
                    GeoShape::from_encoded(lhs_value.data, lhs_value.size));
            if (!shape1) {
                null_map_data[i] = 1;
                return;
            }
            std::unique_ptr<GeoShape> shape2(
                    GeoShape::from_encoded(rhs_value.data, rhs_value.size));
            if (!shape2) {
                null_map_data[i] = 1;
                return;
            }
            res->get_data()[i] = Func::evaluate(shape1.get(), shape2.get());
        };

        ExecuteColumn::execute_binary_compile_time_only_const<TYPE_STRING, TYPE_STRING>(
                left_column, right_column, func);

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StContainsFunc {
    static constexpr auto NAME = "st_contains";
    static bool evaluate(GeoShape* shape1, GeoShape* shape2) { return shape1->contains(shape2); }
};

struct StIntersectsFunc {
    static constexpr auto NAME = "st_intersects";
    static bool evaluate(GeoShape* shape1, GeoShape* shape2) { return shape1->intersects(shape2); }
};

struct StDisjointFunc {
    static constexpr auto NAME = "st_disjoint";
    static bool evaluate(GeoShape* shape1, GeoShape* shape2) { return shape1->disjoint(shape2); }
};

struct StTouchesFunc {
    static constexpr auto NAME = "st_touches";
    static bool evaluate(GeoShape* shape1, GeoShape* shape2) { return shape1->touches(shape2); }
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
    static constexpr auto NAME = Impl::NAME;
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        GeoParseStatus status;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            auto shape = GeoShape::from_wkt(value.data, value.size, status);
            if (shape == nullptr || status != GEO_PARSE_OK ||
                (Impl::shape_type != GEO_SHAPE_ANY && shape->type() != Impl::shape_type)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            buf.clear();
            shape->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
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
    static constexpr auto NAME = Impl::NAME;
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        GeoParseStatus status;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            std::unique_ptr<GeoShape> shape = GeoShape::from_wkb(value.data, value.size, status);
            if (shape == nullptr || status != GEO_PARSE_OK) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            buf.clear();
            shape->encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StAsBinary {
    static constexpr auto NAME = "st_asbinary";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        auto res = ColumnString::create();

        auto col = block.get_by_position(arguments[0]).column;
        const auto size = col->size();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = GeoShape::from_encoded(shape_value.data, shape_value.size);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            std::string binary = GeoShape::as_binary(shape.get());
            if (binary.empty()) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            res->insert_data(binary.data(), binary.size());
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

void register_function_geo(SimpleFunctionFactory& factory) {
    factory.register_function<GeoFunction<StPoint>>();
    factory.register_function<GeoFunction<StAsText<StAsWktName>>>();
    factory.register_function<GeoFunction<StAsText<StAsTextName>>>();
    factory.register_function<GeoFunction<StX>>();
    factory.register_function<GeoFunction<StY>>();
    factory.register_function<GeoFunction<StDistanceSphere>>();
    factory.register_function<GeoFunction<StAngleSphere>>();
    factory.register_function<GeoFunction<StAngle>>();
    factory.register_function<GeoFunction<StAzimuth>>();
    factory.register_function<GeoFunction<StRelationFunction<StContainsFunc>>>();
    factory.register_function<GeoFunction<StRelationFunction<StIntersectsFunc>>>();
    factory.register_function<GeoFunction<StRelationFunction<StDisjointFunc>>>();
    factory.register_function<GeoFunction<StRelationFunction<StTouchesFunc>>>();
    factory.register_function<GeoFunction<StCircle>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeometryFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeomFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineStringFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygon>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygonFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolyFromText>>>();
    factory.register_function<GeoFunction<StAreaSquareMeters>>();
    factory.register_function<GeoFunction<StAreaSquareKm>>();
    factory.register_function<GeoFunction<StGeoFromWkb<StGeometryFromWKB>>>();
    factory.register_function<GeoFunction<StGeoFromWkb<StGeomFromWKB>>>();
    factory.register_function<GeoFunction<StAsBinary>>();
}

} // namespace doris::vectorized
