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
#include <s2/s2point.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

#include "common/cast_set.h"
#include "common/compiler_util.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/geography_calculation.h"
#include "exprs/function/geo/wkb_parse.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function/string_hex_util.h"

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
    if (spatial_type->get_primitive_type() == TYPE_GEOGRAPHY &&
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

    GeoParseStatus status;
    auto shape = GeoShape::from_wkb_bytes(value.data, value.size, status);
    return status == GeoParseStatus::GEO_PARSE_OK ? std::move(shape) : nullptr;
}

static bool any_geo_argument_is_null(const Block& block, const ColumnNumbers& arguments,
                                     size_t row) {
    return std::ranges::any_of(arguments, [&](auto argument) {
        return block.get_by_position(argument).column->is_null_at(row);
    });
}

static Status decode_calculation_shape(StringRef value, const DataTypePtr& type,
                                       std::unique_ptr<GeoShape>* shape,
                                       const char* function_name) {
    *shape = is_spatial_type(type) ? decode_geography_calculation_shape(value)
                                   : decode_geo_shape(value, type);
    // Typed admission rejects unsupported and EMPTY calculation shapes before distance
    // helpers can expose infinity/max sentinels. Raw WKB storage/accessors remain unchanged.
    if (is_spatial_type(type) && *shape == nullptr) {
        return Status::NotSupported("Function {} does not support this Geography WKB shape",
                                    function_name);
    }
    return Status::OK();
}

static bool decode_wkb_hex(StringRef value, std::string* wkb) {
    const char* data = value.data;
    size_t size = value.size;
    if (size >= 2 && ((data[0] == '0' && data[1] == 'x') || (data[0] == '\\' && data[1] == 'x'))) {
        data += 2;
        size -= 2;
    }
    if (size == 0 || (size & 1) != 0) {
        return false;
    }
    wkb->resize(size / 2);
    return string_hex::hex_decode(data, cast_set<ColumnString::Offset>(size), wkb->data()) ==
           static_cast<int>(size / 2);
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
        for (size_t row = 0; row < size; ++row) {
            auto cur_res = point.from_coord(x_col.value_at(row), y_col.value_at(row));
            if (cur_res != GeoParseStatus::GEO_PARSE_OK) {
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
        for (size_t row = 0; row < size; ++row) {
            auto shape_value = input->get_data_at(row);
            if (is_spatial_type(input_type)) {
                std::string wkt;
                if (WkbParse::wkb_to_wkt(shape_value.data, shape_value.size, &wkt) ==
                    GeoParseStatus::GEO_PARSE_OK) {
                    res->insert_data(wkt.data(), wkt.size());
                    continue;
                }
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
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

template <bool X>
struct StCoordinate {
    static constexpr auto NAME = X ? "st_x" : "st_y";
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

        GeoPoint point;
        for (size_t row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            if (is_spatial_type(input_type)) {
                double x;
                double y;
                if (WkbParse::point_coordinates(point_value.data, point_value.size, &x, &y) ==
                    GeoParseStatus::GEO_PARSE_OK) {
                    res->insert_value(X ? x : y);
                    continue;
                }
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            // Legacy STRING points reuse their storage instead of allocating a shape per row.
            if (!point.decode_from(point_value.data, point_value.size)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            res->insert_value(X ? point.x() : point.y());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));

        return Status::OK();
    }
};

using StX = StCoordinate<true>;
using StY = StCoordinate<false>;

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
        for (size_t row = 0; row < size; ++row) {
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

        for (size_t row = 0; row < size; ++row) {
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
        GeoPoint legacy_point1;
        GeoPoint legacy_point2;
        GeoPoint legacy_point3;

        for (size_t row = 0; row < size; ++row) {
            // Propagate NULL before using nested placeholders; a NULL geometry has an
            // empty payload, not a WKB shape.
            if (any_geo_argument_is_null(block, arguments, row)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            std::unique_ptr<GeoShape> point1;
            std::unique_ptr<GeoShape> point2;
            std::unique_ptr<GeoShape> point3;
            auto* pt1 = &legacy_point1;
            auto* pt2 = &legacy_point2;
            auto* pt3 = &legacy_point3;
            // Typed WKB needs shape validation; legacy STRING keeps direct point decoding
            // and reuses the same three objects for the whole block.
            if (is_spatial_type(p1_type)) {
                RETURN_IF_ERROR(
                        decode_calculation_shape(p1->get_data_at(row), p1_type, &point1, NAME));
                RETURN_IF_ERROR(
                        decode_calculation_shape(p2->get_data_at(row), p2_type, &point2, NAME));
                RETURN_IF_ERROR(
                        decode_calculation_shape(p3->get_data_at(row), p3_type, &point3, NAME));
                pt1 = point1 ? dynamic_cast<GeoPoint*>(point1.get()) : nullptr;
                pt2 = point2 ? dynamic_cast<GeoPoint*>(point2.get()) : nullptr;
                pt3 = point3 ? dynamic_cast<GeoPoint*>(point3.get()) : nullptr;
            } else {
                const auto value1 = p1->get_data_at(row);
                const auto value2 = p2->get_data_at(row);
                const auto value3 = p3->get_data_at(row);
                pt1 = legacy_point1.decode_from(value1.data, value1.size) ? pt1 : nullptr;
                pt2 = legacy_point2.decode_from(value2.data, value2.size) ? pt2 : nullptr;
                pt3 = legacy_point3.decode_from(value3.data, value3.size) ? pt3 : nullptr;
            }
            if (pt1 == nullptr || pt2 == nullptr || pt3 == nullptr) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double angle = 0;
            bool computed;
            if (is_spatial_type(p1_type)) {
                double start_bearing;
                double end_bearing;
                computed =
                        geography_bearing(p2->get_data_at(row), p1->get_data_at(row),
                                          &start_bearing) &&
                        geography_bearing(p2->get_data_at(row), p3->get_data_at(row), &end_bearing);
                if (computed) {
                    angle = std::fmod(end_bearing - start_bearing + 2 * M_PI, 2 * M_PI);
                }
            } else {
                computed = GeoPoint::ComputeAngle(pt1, pt2, pt3, &angle);
            }
            if (!computed) {
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
        GeoPoint legacy_point1;
        GeoPoint legacy_point2;
        for (size_t row = 0; row < size; ++row) {
            // Propagate NULL before using nested placeholders; a NULL geometry has an
            // empty payload, not a WKB shape.
            if (any_geo_argument_is_null(block, arguments, row)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            std::unique_ptr<GeoShape> point1;
            std::unique_ptr<GeoShape> point2;
            auto* pt1 = &legacy_point1;
            auto* pt2 = &legacy_point2;
            // Preserve the reusable legacy point path; only typed WKB allocates shapes.
            if (is_spatial_type(left_type)) {
                RETURN_IF_ERROR(decode_calculation_shape(left_col->get_data_at(row), left_type,
                                                         &point1, NAME));
                RETURN_IF_ERROR(decode_calculation_shape(right_col->get_data_at(row), right_type,
                                                         &point2, NAME));
                pt1 = point1 ? dynamic_cast<GeoPoint*>(point1.get()) : nullptr;
                pt2 = point2 ? dynamic_cast<GeoPoint*>(point2.get()) : nullptr;
            } else {
                const auto value1 = left_col->get_data_at(row);
                const auto value2 = right_col->get_data_at(row);
                pt1 = legacy_point1.decode_from(value1.data, value1.size) ? pt1 : nullptr;
                pt2 = legacy_point2.decode_from(value2.data, value2.size) ? pt2 : nullptr;
            }

            if (!(pt1 && pt2)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double angle = 0;
            // Typed longitude/latitude uses the spherical clockwise bearing path.
            const bool computed = is_spatial_type(left_type)
                                          ? geography_bearing(left_col->get_data_at(row),
                                                              right_col->get_data_at(row), &angle)
                                          : GeoPoint::ComputeAzimuth(pt1, pt2, &angle);
            if (!computed) {
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

static bool compute_geo_area(GeoShape& shape, StringRef wkb, const DataTypePtr& input_type,
                             double* area, const std::string& unit) {
    if (is_spatial_type(input_type) && shape.type() == GeoShapeType::GEO_SHAPE_POLYGON) {
        // Use the same stable raw-coordinate edges as typed relations and distance.
        // Measuring the independently rounded GeoPolygon changes near-antipodal arcs.
        // (-45+e, -45) -> (135-e, 45) must retain its area when subdivided at (45, 0).
        *area = geography_area(shape, wkb, unit == "square_meters");
        return true;
    }
    return GeoShape::ComputeArea(&shape, area, unit);
}

template <bool SquareMeters>
struct StArea {
    static constexpr auto NAME = SquareMeters ? "st_area_square_meters" : "st_area_square_km";
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

        for (size_t row = 0; row < size; ++row) {
            // Propagate NULL before using nested placeholders; a NULL geometry has an
            // empty payload, not a WKB shape.
            if (any_geo_argument_is_null(block, arguments, row)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto shape_value = col->get_data_at(row);
            RETURN_IF_ERROR(decode_calculation_shape(shape_value, input_type, &shape, NAME));
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            double area = 0;
            if (!compute_geo_area(*shape, shape_value, input_type, &area,
                                  SquareMeters ? "square_meters" : "square_km")) {
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

using StAreaSquareMeters = StArea<true>;
using StAreaSquareKm = StArea<false>;

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
        for (size_t row = 0; row < size; ++row) {
            auto lng_value = center_lng.value_at(row);
            auto lat_value = center_lat.value_at(row);
            auto radius_value = radius.value_at(row);

            auto value = circle.init(lng_value, lat_value, radius_value);
            if (value != GeoParseStatus::GEO_PARSE_OK) {
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

        for (size_t row = 0; row < size; ++row) {
            // Propagate NULL before using nested placeholders; a NULL geometry has an
            // empty payload, not a WKB shape.
            if (any_geo_argument_is_null(block, arguments, row)) {
                null_map_data[row] = 1;
                continue;
            }
            std::unique_ptr<GeoShape> shape1;
            RETURN_IF_ERROR(
                    decode_calculation_shape(left_col->get_data_at(row), left_type, &shape1, NAME));
            std::unique_ptr<GeoShape> shape2;
            RETURN_IF_ERROR(decode_calculation_shape(right_col->get_data_at(row), right_type,
                                                     &shape2, NAME));

            if (!shape1 || !shape2) {
                null_map_data[row] = 1;
                continue;
            }
            bool relation_value;
            // The legacy relation helpers use planar segments and may close open lines.
            // Dispatch typed Geography separately so date-line edges remain geodesic.
            if (is_spatial_type(left_type)) {
                RETURN_IF_ERROR(geography_relation(NAME, *shape1, *shape2,
                                                   left_col->get_data_at(row),
                                                   right_col->get_data_at(row), &relation_value));
            } else {
                relation_value = Func::evaluate(shape1.get(), shape2.get());
            }
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

struct StEqualsFunc {
    static constexpr auto NAME = "st_equals";
    static bool evaluate(GeoShape*, GeoShape*) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "ST_Equals requires typed Geography");
    }
};

struct StOverlapsFunc {
    static constexpr auto NAME = "st_overlaps";
    static bool evaluate(GeoShape*, GeoShape*) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "ST_Overlaps requires typed Geography");
    }
};

struct StWithinFunc {
    static constexpr auto NAME = "st_within";
    static bool evaluate(GeoShape* shape1, GeoShape* shape2) { return shape1->within(shape2); }
};

struct StGeometryFromText {
    static constexpr auto NAME = "st_geometryfromtext";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_ANY;
};

struct StGeomFromText {
    static constexpr auto NAME = "st_geomfromtext";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_ANY;
};

struct StLineFromText {
    static constexpr auto NAME = "st_linefromtext";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_LINE_STRING;
};

struct StLineStringFromText {
    static constexpr auto NAME = "st_linestringfromtext";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_LINE_STRING;
};

struct StPolygon {
    static constexpr auto NAME = "st_polygon";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_POLYGON;
};

struct StPolyFromText {
    static constexpr auto NAME = "st_polyfromtext";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_POLYGON;
};

struct StPolygonFromText {
    static constexpr auto NAME = "st_polygonfromtext";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_POLYGON;
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
        for (size_t row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            auto shape = GeoShape::from_wkt(value.data, value.size, status);
            if (shape == nullptr || status != GeoParseStatus::GEO_PARSE_OK ||
                (Impl::shape_type != GeoShapeType::GEO_SHAPE_ANY &&
                 shape->type() != Impl::shape_type)) {
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
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_ANY;
    static constexpr PrimitiveType OUTPUT_TYPE = TYPE_STRING;
};

struct StGeomFromWKB {
    static constexpr auto NAME = "st_geomfromwkb";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_ANY;
    static constexpr PrimitiveType OUTPUT_TYPE = TYPE_STRING;
};

struct StGeogFromWKB {
    static constexpr auto NAME = "st_geogfromwkb";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_ANY;
    static constexpr PrimitiveType OUTPUT_TYPE = TYPE_GEOGRAPHY;
};

struct StGeometryFromWKBTyped {
    static constexpr auto NAME = "st_geometryfromwkbtyped";
    static constexpr GeoShapeType shape_type = GeoShapeType::GEO_SHAPE_ANY;
    static constexpr PrimitiveType OUTPUT_TYPE = TYPE_GEOMETRY;
};

template <typename Impl>
struct LegacyStGeoFromWkb {
    static constexpr auto NAME = Impl::NAME;
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        GeoParseStatus status;
        std::string buf;
        for (size_t row = 0; row < size; ++row) {
            const auto value = geo->get_data_at(row);
            auto shape = GeoShape::from_wkb(value.data, value.size, status);
            if (shape == nullptr || status != GeoParseStatus::GEO_PARSE_OK) {
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

template <typename Impl>
struct StGeoFromWkb {
    static constexpr auto NAME = Impl::NAME;
    static constexpr PrimitiveType OUTPUT_TYPE = Impl::OUTPUT_TYPE;
    static const size_t NUM_ARGS = 1;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto& geo = block.get_by_position(arguments[0]).column;

        const auto size = geo->size();
        auto res = ColumnSpatial::create(Impl::OUTPUT_TYPE);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        GeoParseStatus status;
        std::string wkb;
        for (size_t row = 0; row < size; ++row) {
            auto value = geo->get_data_at(row);
            if (!decode_wkb_hex(value, &wkb)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            const auto& spatial = assert_cast<const DataTypeSpatial&>(
                    *remove_nullable(block.get_data_type(result)));
            status = WkbParse::validate_wkb_bytes(wkb.data(), wkb.size(),
                                                  spatial.requires_geographic_coordinates());
            if (status != GeoParseStatus::GEO_PARSE_OK) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            res->insert_data(wkb.data(), wkb.size());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

template <typename Impl>
class SpatialWkbConstructorFunction : public IFunction {
public:
    static constexpr auto name = Impl::NAME;
    static FunctionPtr create() { return std::make_shared<SpatialWkbConstructorFunction<Impl>>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return Impl::NUM_ARGS; }
    bool is_variadic() const override { return false; }
    // FE owns parameterized spatial metadata; old plans may still request encoded STRING.
    bool skip_return_type_check() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        if constexpr (Impl::OUTPUT_TYPE == TYPE_STRING) {
            return make_nullable(std::make_shared<DataTypeString>());
        }
        if constexpr (Impl::OUTPUT_TYPE == TYPE_GEOGRAPHY) {
            return make_nullable(
                    std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "spherical"));
        }
        return make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY));
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t) const override {
        if (!is_spatial_type(block.get_data_type(result))) {
            return LegacyStGeoFromWkb<Impl>::execute(block, arguments, result);
        }
        return Impl::execute(block, arguments, result);
    }
};

struct StAsBinary {
    static constexpr auto NAME = "st_asbinary";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeString;
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);
        auto return_type = block.get_data_type(result);
        // Raw typed WKB is binary, including byte sequences that are not valid UTF-8.
        MutableColumnPtr res = is_spatial_type(block.get_data_type(arguments[0]))
                                       ? MutableColumnPtr(ColumnVarbinary::create())
                                       : MutableColumnPtr(ColumnString::create());

        auto col = block.get_by_position(arguments[0]).column;
        const auto& input_type = block.get_data_type(arguments[0]);
        const auto size = col->size();
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();
        std::unique_ptr<GeoShape> shape;

        for (size_t row = 0; row < size; ++row) {
            // NULL has no shape bytes: preserve its null bit instead of decoding the
            // empty nested payload as an invalid geometry.
            if (col->is_null_at(row)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto shape_value = col->get_data_at(row);
            if (is_spatial_type(input_type)) {
                res->insert_data(shape_value.data, shape_value.size);
                continue;
            }
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

class StAsBinaryFunction final : public SpatialWkbGeoFunction<StAsBinary> {
public:
    static FunctionPtr create() { return std::make_shared<StAsBinaryFunction>(); }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (is_spatial_type(arguments[0])) {
            return make_nullable(std::make_shared<DataTypeVarbinary>());
        }
        return GeoFunction<StAsBinary>::get_return_type_impl(arguments);
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
        for (size_t row = 0; row < size; ++row) {
            // Propagate NULL before using nested placeholders; a NULL geometry has an
            // empty payload, not a WKB shape.
            if (any_geo_argument_is_null(block, arguments, row)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            auto shape_value = col->get_data_at(row);
            RETURN_IF_ERROR(decode_calculation_shape(shape_value, input_type, &shape, NAME));
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
        for (size_t row = 0; row < size; ++row) {
            auto shape_value = col->get_data_at(row);
            if (is_spatial_type(input_type)) {
                std::string geo_type;
                if (WkbParse::geometry_type(shape_value.data, shape_value.size, &geo_type) ==
                    GeoParseStatus::GEO_PARSE_OK) {
                    res->insert_data(geo_type.data(), geo_type.size());
                    continue;
                }
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
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

struct StIsClosed {
    static constexpr auto NAME = "st_isclosed";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeUInt8;

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);

        auto col = ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[0]).column);
        const auto size = col.size();

        auto res = ColumnUInt8::create(size, 0);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& result_data = res->get_data();
        auto& null_map_data = null_map->get_data();

        GeoLine line;
        for (size_t row = 0; row < size; ++row) {
            auto value = col.value_at(row);
            if (!line.decode_from(value.data, value.size)) {
                null_map_data[row] = 1;
                continue;
            }

            const auto num_points = line.numPoint();
            if (num_points < 2) {
                null_map_data[row] = 1;
                continue;
            }

            result_data[row] = *line.getPoint(0) == *line.getPoint(num_points - 1);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

template <bool Within>
struct StDistanceFunction {
    static constexpr auto NAME = Within ? "st_dwithin" : "st_distance";
    static constexpr size_t NUM_ARGS = Within ? 3 : 2;
    using Type = std::conditional_t<Within, DataTypeUInt8, DataTypeFloat64>;

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), NUM_ARGS);
        auto return_type = block.get_data_type(result);
        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto& left_type = block.get_data_type(arguments[0]);
        const auto& right_type = block.get_data_type(arguments[1]);

        RETURN_IF_ERROR(validate_geography_semantics(left_type, NAME));
        RETURN_IF_ERROR(validate_geography_semantics(right_type, NAME));

        const auto size = block.rows();

        auto res = Type::ColumnType::create();
        std::optional<ColumnView<TYPE_DOUBLE>> threshold_column;
        if constexpr (Within) {
            threshold_column.emplace(
                    ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[2]).column));
        }
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        std::unique_ptr<GeoShape> left_shape;
        std::unique_ptr<GeoShape> right_shape;
        bool left_decoded = false;
        bool right_decoded = false;
        for (size_t row = 0; row < size; ++row) {
            // Propagate NULL before using nested placeholders; a NULL geometry has an
            // empty payload, not a WKB shape.
            if (any_geo_argument_is_null(block, arguments, row)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            double threshold = 0;
            if constexpr (Within) {
                threshold = threshold_column->value_at(row);
                // A dynamic negative/NaN/infinite radius is not a valid distance threshold.
                if (!std::isfinite(threshold) || threshold < 0) {
                    return Status::InvalidArgument(
                            "ST_DWithin distance must be finite and nonnegative");
                }
            }
            if (!left_const || !left_decoded) {
                RETURN_IF_ERROR(
                        decode_calculation_shape(left_column->get_data_at(left_const ? 0 : row),
                                                 left_type, &left_shape, NAME));
                left_decoded = true;
            }
            if (!right_const || !right_decoded) {
                RETURN_IF_ERROR(
                        decode_calculation_shape(right_column->get_data_at(right_const ? 0 : row),
                                                 right_type, &right_shape, NAME));
                right_decoded = true;
            }
            if (!left_shape || !right_shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            const double distance =
                    is_spatial_type(left_type)
                            ? geography_distance(*left_shape, *right_shape,
                                                 left_column->get_data_at(left_const ? 0 : row),
                                                 right_column->get_data_at(right_const ? 0 : row))
                            : left_shape->Distance(right_shape.get());
            if (UNLIKELY(distance < 0)) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }
            if constexpr (Within) {
                res->insert_value(distance <= threshold);
            } else {
                res->insert_value(distance);
            }
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct StNumGeometries {
    static constexpr auto NAME = "st_numgeometries";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeInt64;

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);

        auto col = ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[0]).column);
        const auto size = col.size();

        auto res = ColumnInt64::create();
        res->reserve(size);

        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        for (size_t row = 0; row < size; ++row) {
            auto value = col.value_at(row);
            auto shape = GeoShape::from_encoded(value.data, value.size);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            res->insert_value(shape->num_geometries());
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

class FunctionStGeometries final : public IFunction {
public:
    static constexpr auto name = "st_geometries";

    static FunctionPtr create() { return std::make_shared<FunctionStGeometries>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 1);

        auto col = ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[0]).column);
        const auto size = col.size();

        auto nested_data = ColumnString::create();
        auto offsets_col = ColumnArray::ColumnOffsets::create();
        auto& offsets = offsets_col->get_data();
        offsets.reserve(size);

        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        size_t current_offset = 0;
        std::string buf;

        for (size_t row = 0; row < size; ++row) {
            auto shape_value = col.value_at(row);
            auto shape = GeoShape::from_encoded(shape_value.data, shape_value.size);

            if (!shape) {
                null_map_data[row] = 1;
                offsets.push_back(current_offset);
                continue;
            }

            if (shape->type() == GeoShapeType::GEO_SHAPE_MULTI_POLYGON) {
                auto* multi_polygon = static_cast<GeoMultiPolygon*>(shape.get());
                const auto& polygons = multi_polygon->polygons();

                if (polygons.empty()) {
                    null_map_data[row] = 1;
                    offsets.push_back(current_offset);
                    continue;
                }

                for (const auto& polygon : polygons) {
                    DCHECK(polygon != nullptr);
                    buf.clear();
                    polygon->encode_to(&buf);
                    nested_data->insert_data(buf.data(), buf.size());
                    ++current_offset;
                }
            } else {
                nested_data->insert_data(shape_value.data, shape_value.size);
                ++current_offset;
            }

            offsets.push_back(current_offset);
        }

        auto nested_null_map = ColumnUInt8::create(nested_data->size(), 0);
        auto nested_nullable =
                ColumnNullable::create(std::move(nested_data), std::move(nested_null_map));
        auto array_col = ColumnArray::create(std::move(nested_nullable), std::move(offsets_col));

        block.replace_by_position(
                result, ColumnNullable::create(std::move(array_col), std::move(null_map)));

        return Status::OK();
    }
};

struct StNumPoints {
    static constexpr auto NAME = "st_numpoints";
    static const size_t NUM_ARGS = 1;
    using Type = DataTypeInt64;

    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        DCHECK_EQ(arguments.size(), 1);

        auto col = ColumnView<TYPE_STRING>::create(block.get_by_position(arguments[0]).column);
        const auto size = col.size();

        auto res = ColumnInt64::create();
        res->reserve(size);

        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        for (size_t row = 0; row < size; ++row) {
            auto value = col.value_at(row);
            auto shape = GeoShape::from_encoded(value.data, value.size);
            if (!shape) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            auto num_points = shape->num_points();
            if (num_points < 0) {
                null_map_data[row] = 1;
                res->insert_default();
                continue;
            }

            res->insert_value(num_points);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

void register_function_h3(SimpleFunctionFactory& factory);

void register_function_geo(SimpleFunctionFactory& factory) {
    register_function_h3(factory);
    factory.register_function<GeoFunction<StPoint>>();
    factory.register_function<SpatialWkbGeoFunction<StAsText<StAsWktName>>>();
    factory.register_function<SpatialWkbGeoFunction<StAsText<StAsTextName>>>();
    factory.register_function<SpatialWkbGeoFunction<StX>>();
    factory.register_function<SpatialWkbGeoFunction<StY>>();
    factory.register_function<GeoFunction<StDistanceSphere>>();
    factory.register_function<GeoFunction<StAngleSphere>>();
    factory.register_function<SpatialWkbGeoFunction<StAngle>>();
    factory.register_function<SpatialWkbGeoFunction<StAzimuth>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StContainsFunc>>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StIntersectsFunc>>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StDisjointFunc>>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StTouchesFunc>>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StWithinFunc>>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StEqualsFunc>>>();
    factory.register_function<SpatialWkbGeoFunction<StRelationFunction<StOverlapsFunc>>>();
    factory.register_function<GeoFunction<StCircle>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeometryFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StGeomFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StLineStringFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygon>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolygonFromText>>>();
    factory.register_function<GeoFunction<StGeoFromText<StPolyFromText>>>();
    factory.register_function<SpatialWkbGeoFunction<StAreaSquareMeters>>();
    factory.register_function<SpatialWkbGeoFunction<StAreaSquareKm>>();
    factory.register_function<SpatialWkbConstructorFunction<StGeoFromWkb<StGeometryFromWKB>>>();
    factory.register_function<SpatialWkbConstructorFunction<StGeoFromWkb<StGeomFromWKB>>>();
    factory.register_function<
            SpatialWkbConstructorFunction<StGeoFromWkb<StGeometryFromWKBTyped>>>();
    factory.register_function<SpatialWkbConstructorFunction<StGeoFromWkb<StGeogFromWKB>>>();
    factory.register_function<StAsBinaryFunction>();
    factory.register_function<SpatialWkbGeoFunction<StLength>>();
    factory.register_function<SpatialWkbGeoFunction<StGeometryType>>();
    factory.register_function<SpatialWkbGeoFunction<StDistanceFunction<false>>>();
    factory.register_function<SpatialWkbGeoFunction<StDistanceFunction<true>>>();
    factory.register_function<GeoFunction<StIsClosed>>();
    factory.register_function<GeoFunction<StNumGeometries>>();
    factory.register_function<GeoFunction<StNumPoints>>();
    factory.register_alias("st_numpoints", "st_npoints");
    factory.register_function<FunctionStGeometries>();
}

} // namespace doris
