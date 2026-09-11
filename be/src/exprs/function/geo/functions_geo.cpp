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

#include "exprs/function/geo/functions_geo.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <utility>

#include "common/compiler_util.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

static bool is_spatial_type(const DataTypePtr& type) {
    const auto primitive_type = remove_nullable(type)->get_primitive_type();
    return primitive_type == TYPE_GEOMETRY || primitive_type == TYPE_GEOGRAPHY;
}

static Status validate_geography_semantics(const DataTypePtr& type, const char* function_name) {
    const auto& nested_type = remove_nullable(type);
    if (!is_spatial_type(nested_type)) {
        return Status::OK();
    }

    const auto* spatial_type = dynamic_cast<const DataTypeSpatial*>(nested_type.get());
    DCHECK(spatial_type != nullptr);
    if (spatial_type != nullptr && spatial_type->get_primitive_type() == TYPE_GEOGRAPHY &&
        spatial_type->crs() == "OGC:CRS84" && spatial_type->algorithm() == "spherical") {
        return Status::OK();
    }
    return Status::NotSupported(
            "Function {} requires GEOGRAPHY(OGC:CRS84, spherical) for spatial inputs",
            function_name);
}

static std::unique_ptr<GeoShape> decode_geo_shape(StringRef value, const DataTypePtr& type) {
    if (!is_spatial_type(type)) {
        return GeoShape::from_encoded(value.data, value.size);
    }

    static constexpr char HEX[] = "0123456789ABCDEF";
    std::string hex_wkb;
    hex_wkb.reserve(value.size * 2);
    for (size_t i = 0; i < value.size; ++i) {
        const auto byte = static_cast<unsigned char>(value.data[i]);
        hex_wkb.push_back(HEX[byte >> 4]);
        hex_wkb.push_back(HEX[byte & 0x0F]);
    }

    GeoParseStatus status;
    auto shape = GeoShape::from_wkb(hex_wkb.data(), hex_wkb.size(), status);
    return status == GEO_PARSE_OK ? std::move(shape) : nullptr;
}

struct StPoint {
    static constexpr auto NAME = "st_point";
    static const size_t NUM_ARGS = 2;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);

        auto x_col = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[0]).column);
        auto y_col = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[1]).column);

        const auto size = x_col.size();

        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        GeoPoint point;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(x_col.value_at(row), y_col.value_at(row));
            if (cur_res != GEO_PARSE_OK) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            buf.clear();
            point.encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }

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
        const auto& input_type = block.get_data_type(arguments[0]);

        auto size = input->size();

        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = input->get_data_at(row);
            shape = decode_geo_shape(shape_value, input_type);
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
        const auto& input_type = block.get_data_type(arguments[0]);

        auto size = input->size();

        auto res = ColumnFloat64::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        res->reserve(size);

        for (int row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            auto shape = decode_geo_shape(point_value, input_type);
            auto* point = shape ? dynamic_cast<GeoPoint*>(shape.get()) : nullptr;
            if (point == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto x_value = point->x();
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
        const auto& input_type = block.get_data_type(arguments[0]);

        auto size = input->size();

        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        for (int row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            auto shape = decode_geo_shape(point_value, input_type);
            auto* point = shape ? dynamic_cast<GeoPoint*>(shape.get()) : nullptr;
            if (point == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto y_value = point->y();
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

        const auto& p1 = block.get_by_position(arguments[0]).column;
        const auto& p2 = block.get_by_position(arguments[1]).column;
        const auto& p3 = block.get_by_position(arguments[2]).column;
        const auto& p1_type = block.get_data_type(arguments[0]);
        const auto& p2_type = block.get_data_type(arguments[1]);
        const auto& p3_type = block.get_data_type(arguments[2]);
        RETURN_IF_ERROR(validate_geography_semantics(p1_type, NAME));
        RETURN_IF_ERROR(validate_geography_semantics(p2_type, NAME));
        RETURN_IF_ERROR(validate_geography_semantics(p3_type, NAME));
        const auto size = p1->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        for (int row = 0; row < size; ++row) {
            auto point1 = decode_geo_shape(p1->get_data_at(row), p1_type);
            auto* pt1 = point1 ? dynamic_cast<GeoPoint*>(point1.get()) : nullptr;
            if (pt1 == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            auto point2 = decode_geo_shape(p2->get_data_at(row), p2_type);
            auto* pt2 = point2 ? dynamic_cast<GeoPoint*>(point2.get()) : nullptr;
            if (pt2 == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto point3 = decode_geo_shape(p3->get_data_at(row), p3_type);
            auto* pt3 = point3 ? dynamic_cast<GeoPoint*>(point3.get()) : nullptr;
            if (pt3 == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double angle = 0;
            if (!GeoPoint::ComputeAngle(pt1, pt2, pt3, &angle)) {
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

struct StAzimuth {
    static constexpr auto NAME = "st_azimuth";
    static const size_t NUM_ARGS = 2;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);

        const auto& left_col = block.get_by_position(arguments[0]).column;
        const auto& right_col = block.get_by_position(arguments[1]).column;
        const auto& left_type = block.get_data_type(arguments[0]);
        const auto& right_type = block.get_data_type(arguments[1]);
        RETURN_IF_ERROR(validate_geography_semantics(left_type, NAME));
        RETURN_IF_ERROR(validate_geography_semantics(right_type, NAME));

        const auto size = left_col->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        for (int row = 0; row < size; ++row) {
            auto point1 = decode_geo_shape(left_col->get_data_at(row), left_type);
            auto point2 = decode_geo_shape(right_col->get_data_at(row), right_type);
            auto* pt1 = point1 ? dynamic_cast<GeoPoint*>(point1.get()) : nullptr;
            auto* pt2 = point2 ? dynamic_cast<GeoPoint*>(point2.get()) : nullptr;

            if (!(pt1 && pt2)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double angle = 0;
            if (!GeoPoint::ComputeAzimuth(pt1, pt2, &angle)) {
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

struct StAreaSquareMeters {
    static constexpr auto NAME = "st_area_square_meters";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& input_type = block.get_data_type(arguments[0]);
        RETURN_IF_ERROR(validate_geography_semantics(input_type, NAME));
        const auto size = col->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = decode_geo_shape(shape_value, input_type);
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
        const auto& input_type = block.get_data_type(arguments[0]);
        RETURN_IF_ERROR(validate_geography_semantics(input_type, NAME));
        const auto size = col->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = decode_geo_shape(shape_value, input_type);
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
        auto center_lng =
                ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[0]).column);
        auto center_lat =
                ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[1]).column);
        auto radius = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[2]).column);

        const auto size = center_lng.size();

        auto res = ColumnString::create();

        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        GeoCircle circle;
        std::string buf;
        for (int row = 0; row < size; ++row) {
            auto lng_value = center_lng.value_at(row);
            auto lat_value = center_lat.value_at(row);
            auto radius_value = radius.value_at(row);

            auto value = circle.init(lng_value, lat_value, radius_value);
            if (value != GEO_PARSE_OK) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            buf.clear();
            circle.encode_to(&buf);
            res->insert_data(buf.data(), buf.size());
        }
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
        const auto& left_col = block.get_by_position(arguments[0]).column;
        const auto& right_col = block.get_by_position(arguments[1]).column;
        const auto& left_type = block.get_data_type(arguments[0]);
        const auto& right_type = block.get_data_type(arguments[1]);
        RETURN_IF_ERROR(validate_geography_semantics(left_type, NAME));
        RETURN_IF_ERROR(validate_geography_semantics(right_type, NAME));

        const auto size = left_col->size();

        auto res = ColumnUInt8::create(size, 0);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        for (int row = 0; row < size; ++row) {
            auto shape1 = decode_geo_shape(left_col->get_data_at(row), left_type);
            auto shape2 = decode_geo_shape(right_col->get_data_at(row), right_type);

            if (!shape1 || !shape2) {
                null_map_data[row] = 1;
                continue;
            }
            auto relation_value = Func::evaluate(shape1.get(), shape2.get());
            res->get_data()[row] = relation_value;
        }
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
        const auto& input_type = block.get_data_type(arguments[0]);
        const auto size = col->size();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        std::unique_ptr<GeoShape> shape;

        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = decode_geo_shape(shape_value, input_type);
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

struct StLength {
    static constexpr auto NAME = "st_length";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeFloat64;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& input_type = block.get_data_type(arguments[0]);
        RETURN_IF_ERROR(validate_geography_semantics(input_type, NAME));
        const auto size = col->size();
        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = decode_geo_shape(shape_value, input_type);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double length = shape->Length();
            res->insert_value(length);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StGeometryType {
    static constexpr auto NAME = "st_geometrytype";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);

        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& input_type = block.get_data_type(arguments[0]);
        const auto size = col->size();
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> shape;
        for (int row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            shape = decode_geo_shape(shape_value, input_type);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            auto geo_type = shape->GeometryType();
            res->insert_data(geo_type.data(), geo_type.size());
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StDistance {
    static constexpr auto NAME = "st_distance";
    static const size_t NUM_ARGS = 2;
    using Type = DataTypeFloat64;

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 2);
        auto return_type = block.get_data_type(result);
        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto& left_type = block.get_data_type(arguments[0]);
        const auto& right_type = block.get_data_type(arguments[1]);

        RETURN_IF_ERROR(validate_geography_semantics(left_type, NAME));
        RETURN_IF_ERROR(validate_geography_semantics(right_type, NAME));

        const auto size = std::max(left_column->size(), right_column->size());

        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        if (left_const || right_const) {
            const auto& const_column = left_const ? left_column : right_column;
            const auto& const_type = left_const ? left_type : right_type;
            auto const_shape = decode_geo_shape(const_column->get_data_at(0), const_type);
            if (!const_shape) {
                for (int row = 0; row < size; ++row) {
                    null_map_data[row] = 1;
                    res->insert_default();
                }
            } else {
                const auto& vector_column = left_const ? right_column : left_column;
                const auto& vector_type = left_const ? right_type : left_type;
                for (int row = 0; row < size; ++row) {
                    auto vector_shape =
                            decode_geo_shape(vector_column->get_data_at(row), vector_type);
                    if (!vector_shape) {
                        null_map_data[row] = 1;
                        res->insert_default();
                        continue;
                    }
                    const double distance = left_const ? const_shape->Distance(vector_shape.get())
                                                       : vector_shape->Distance(const_shape.get());
                    if (UNLIKELY(distance < 0)) {
                        null_map_data[row] = 1;
                        res->insert_default();
                        continue;
                    }
                    res->insert_value(distance);
                }
            }
        } else {
            for (int row = 0; row < size; ++row) {
                auto left_shape = decode_geo_shape(left_column->get_data_at(row), left_type);
                auto right_shape = decode_geo_shape(right_column->get_data_at(row), right_type);
                if (!left_shape || !right_shape) {
                    null_map_data[row] = 1;
                    res->insert_default();
                    continue;
                }
                const double distance = left_shape->Distance(right_shape.get());
                if (UNLIKELY(distance < 0)) {
                    null_map_data[row] = 1;
                    res->insert_default();
                    continue;
                }
                res->insert_value(distance);
            }
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
    factory.register_function<GeoFunction<StLength>>();
    factory.register_function<GeoFunction<StGeometryType>>();
    factory.register_function<GeoFunction<StDistance>>();
}

} // namespace doris
