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
#include <s2/mutable_s2shape_index.h>
#include <s2/s2boolean_operation.h>
#include <s2/s2builderutil_s2polyline_vector_layer.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2lax_polygon_shape.h>
#include <s2/s2lax_polyline_shape.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/s2point_vector_shape.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/s2predicates.h>

#include <algorithm>
#include <array>
#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
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
    *shape = decode_geo_shape(value, type);
    // POLYGON EMPTY decodes successfully, but distance helpers retain infinity/max sentinels.
    // Reject it for typed calculations just like empty points/lines, leaving raw WKB untouched.
    if (is_spatial_type(type) &&
        (*shape == nullptr ||
         ((*shape)->type() == GeoShapeType::GEO_SHAPE_POLYGON &&
          assert_cast<const GeoPolygon&>(**shape).polygon()->num_loops() == 0))) {
        return Status::NotSupported("Function {} does not support this Geography WKB shape",
                                    function_name);
    }
    return Status::OK();
}

// Calculations use canonical positions; raw spatial accessors still return the original WKB.
static S2Point canonical_geography_point(const S2Point& point) {
    const auto coordinate = S2LatLng(point);
    const double latitude = coordinate.lat().radians();
    // All longitudes at a pole and +/-180 at the date line denote the same position.
    if (std::abs(latitude) == M_PI_2) {
        return {0, 0, latitude > 0 ? 1.0 : -1.0};
    }
    if (coordinate.lng().radians() == M_PI) {
        // Flip only sin(longitude): rebuilding latitude can change its last bit.
        return {point.x(), -point.y(), point.z()};
    }
    return point;
}

// S2 predicates operate on exact rounded vectors. Splitting a meridian at a new latitude
// can otherwise turn one common edge into two almost-coplanar curves. Node only edges whose
// shared great circle is proved by original WKB ordinates; never snap nearby parallel curves.
class GeographyEdgeNodes {
public:
    GeographyEdgeNodes(StringRef left, StringRef right) {
        GeoCoordinateList coordinates;
        const auto left_status = WkbParse::xy_coordinates(left.data, left.size, &coordinates);
        const auto right_status = WkbParse::xy_coordinates(right.data, right.size, &coordinates);
        DCHECK_EQ(left_status, GeoParseStatus::GEO_PARSE_OK);
        DCHECK_EQ(right_status, GeoParseStatus::GEO_PARSE_OK);
        for (auto coordinate : coordinates.list) {
            if (coordinate.x == 180) {
                coordinate.x = -180;
            }
            const auto point = canonical_geography_point(
                    S2LatLng::FromDegrees(coordinate.y, coordinate.x).ToPoint());
            _coordinates.emplace(point, coordinate);
            _vertex_index.Add(point, 0);
            if (std::abs(coordinate.y) != 90) {
                _meridians[meridian(coordinate.x)][coordinate.x < 0].push_back(
                        {coordinate.y, point});
            }
        }
        for (auto& entry : _meridians) {
            for (auto& side : entry.second) {
                std::ranges::sort(side, {}, &Vertex::latitude);
                const auto duplicate = std::ranges::unique(side, {}, &Vertex::point);
                side.erase(duplicate.begin(), duplicate.end());
            }
        }
    }

    void append_edge(const S2Point& raw_start, const S2Point& raw_end,
                     std::vector<S2Point>* output) const {
        const auto start = canonical_geography_point(raw_start);
        const auto end = canonical_geography_point(raw_end);
        output->push_back(start);
        // Different raw longitudes at one pole denote the same calculation point.
        if (start == end) {
            return;
        }
        const auto& a = _coordinates.at(start);
        const auto& b = _coordinates.at(end);
        const bool a_pole = std::abs(a.y) == 90;
        const bool b_pole = std::abs(b.y) == 90;
        const auto key = meridian(a_pole ? b.x : a.x);
        const bool common_meridian = a_pole || b_pole || key == meridian(b.x);
        // Exact antipodes have no unique edge; rounded S2 vectors can conceal this ambiguity.
        if (a.y == -b.y && common_meridian && ((a_pole && b_pole) || (a.x < 0) != (b.x < 0))) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Geography calculations do not support antipodal edges");
        }
        if (common_meridian) {
            const bool a_side = (a_pole ? b.x : a.x) < 0;
            const bool b_side = (b_pole ? a.x : b.x) < 0;
            const auto& group = _meridians.at(key);
            if (a_side == b_side) {
                append_range(group[a_side], a.y, b.y, output);
            } else {
                // (10,80) -> (-170,80) passes through the north pole. Keep latitude
                // as-is on each side: computing 180-latitude would erase tiny ordinates.
                const double latitude = a.y > -b.y ? 90 : -90;
                append_range(group[a_side], a.y, latitude, output);
                output->emplace_back(0, 0, latitude > 0 ? 1 : -1);
                append_range(group[b_side], latitude, b.y, output);
            }
            return;
        }
        // For other great circles, including the equator, node only exact coplanar
        // points on the edge. The query radius selects candidates, never equality.
        S2ClosestPointQuery<int> query(&_vertex_index);
        query.mutable_options()->set_conservative_max_distance(
                S1Angle::Radians(8 * std::numeric_limits<double>::epsilon()));
        S2ClosestPointQuery<int>::EdgeTarget target(start, end);
        std::vector<S2Point> on_edge;
        for (const auto& candidate : query.FindClosestPoints(&target)) {
            const auto& point = candidate.point();
            if (point != start && point != end &&
                s2pred::ExpensiveSign(start, end, point, false) == 0 &&
                s2pred::CompareEdgeDistance(point, start, end, S1ChordAngle::Zero()) <= 0) {
                on_edge.push_back(point);
            }
        }
        // Rounded angles can tie near the middle of a long edge, reversing adjacent nodes.
        std::ranges::sort(on_edge, [&](const S2Point& a, const S2Point& b) {
            return s2pred::CompareDistances(start, a, b) < 0;
        });
        for (const auto& point : on_edge) {
            if (output->back() != point) {
                output->push_back(point);
            }
        }
    }

private:
    struct Vertex {
        double latitude;
        S2Point point;
    };
    static std::pair<double, double> meridian(double longitude) {
        if (longitude >= 0) {
            return {longitude, 0};
        }
        // Error-free TwoSum: -1e-15+180 and -2e-15+180 both round to 180.
        // Retain the residual so distinct original meridians never become one.
        const double high = longitude + 180;
        const double offset = high - 180;
        const double low = (180 - (high - offset)) + (longitude - offset);
        return {high, low};
    }
    static void append_range(const std::vector<Vertex>& vertices, double from, double to,
                             std::vector<S2Point>* output) {
        if (from == to) {
            return;
        }
        auto first = std::ranges::upper_bound(vertices, std::min(from, to), {}, &Vertex::latitude);
        auto last = std::ranges::lower_bound(vertices, std::max(from, to), {}, &Vertex::latitude);
        if (from < to) {
            for (auto it = first; it != last; ++it) {
                output->push_back(it->point);
            }
        } else {
            while (last != first) {
                output->push_back((--last)->point);
            }
        }
    }
    std::map<S2Point, GeoCoordinate> _coordinates;
    S2PointIndex<int> _vertex_index;
    std::map<std::pair<double, double>, std::array<std::vector<Vertex>, 2>> _meridians;
};

static void geography_shape_index(const GeoShape& shape, const GeographyEdgeNodes& nodes,
                                  MutableS2ShapeIndex* index) {
    switch (shape.type()) {
    case GeoShapeType::GEO_SHAPE_POINT:
        index->Add(std::make_unique<S2PointVectorShape>(std::vector<S2Point> {
                canonical_geography_point(*assert_cast<const GeoPoint&>(shape).point())}));
        break;
    case GeoShapeType::GEO_SHAPE_LINE_STRING: {
        const auto* line = assert_cast<const GeoLine&>(shape).polyline();
        std::vector<S2Point> vertices;
        for (int i = 0; i + 1 < line->num_vertices(); ++i) {
            nodes.append_edge(line->vertex(i), line->vertex(i + 1), &vertices);
        }
        vertices.push_back(canonical_geography_point(line->vertex(line->num_vertices() - 1)));
        index->Add(std::make_unique<S2LaxPolylineShape>(vertices));
        break;
    }
    case GeoShapeType::GEO_SHAPE_POLYGON: {
        const auto* polygon = assert_cast<const GeoPolygon&>(shape).polygon();
        std::vector<S2LaxPolygonShape::Loop> loops;
        for (int i = 0; i < polygon->num_loops(); ++i) {
            auto& vertices = loops.emplace_back();
            const auto* loop = polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                nodes.append_edge(loop->oriented_vertex(j),
                                  loop->oriented_vertex((j + 1) % loop->num_vertices()), &vertices);
            }
        }
        index->Add(std::make_unique<S2LaxPolygonShape>(loops));
        break;
    }
    default:
        // decode_calculation_shape admits only nonempty Point/LineString/Polygon WKB.
        throw Exception(ErrorCode::INTERNAL_ERROR, "Unexpected Geography calculation shape");
    }
}

static S1ChordAngle geography_conversion_error() {
    // S2LatLng::ToPoint and degrees-to-radians rounding can separate positions on
    // the same meridian. This is about 1e-8 metres, not a user snap radius.
    return S1ChordAngle(S1Angle::Radians(8 * std::numeric_limits<double>::epsilon()));
}

static bool geography_point_relation(std::string_view name, const GeoShape& lhs,
                                     const GeoShape& rhs) {
    const auto& point_shape = lhs.type() == GeoShapeType::GEO_SHAPE_POINT ? lhs : rhs;
    const auto& other = lhs.type() == GeoShapeType::GEO_SHAPE_POINT ? rhs : lhs;
    const auto point =
            canonical_geography_point(*assert_cast<const GeoPoint&>(point_shape).point());
    bool interior = false;
    bool boundary = false;
    // Allow only conversion-scale error for the point and edge endpoints:
    // POINT(10 45.5) is on LINESTRING(10 45,10 46), but their rounded vectors
    // are not exactly coplanar.
    const auto conversion_error = geography_conversion_error();
    if (other.type() == GeoShapeType::GEO_SHAPE_POINT) {
        interior = point == canonical_geography_point(*assert_cast<const GeoPoint&>(other).point());
    } else if (other.type() == GeoShapeType::GEO_SHAPE_LINE_STRING) {
        const auto* line = assert_cast<const GeoLine&>(other).polyline();
        bool member = false;
        // S2 Boolean operations symbolically move points off edge interiors. Use its
        // unperturbed distance predicate on every real geodesic edge instead.
        for (int i = 0; i + 1 < line->num_vertices(); ++i) {
            member = member ||
                     s2pred::CompareEdgeDistance(point, canonical_geography_point(line->vertex(i)),
                                                 canonical_geography_point(line->vertex(i + 1)),
                                                 conversion_error) <= 0;
        }
        const auto first = canonical_geography_point(line->vertex(0));
        const auto last = canonical_geography_point(line->vertex(line->num_vertices() - 1));
        boundary = first != last && (point == first || point == last);
        interior = member && !boundary;
    } else {
        DCHECK(other.type() == GeoShapeType::GEO_SHAPE_POLYGON);
        const auto* polygon = assert_cast<const GeoPolygon&>(other).polygon();
        // Polygon rings have closing edges; open polylines above do not.
        for (int i = 0; i < polygon->num_loops(); ++i) {
            const auto* loop = polygon->loop(i);
            for (int j = 0; j < loop->num_vertices(); ++j) {
                boundary = boundary || s2pred::CompareEdgeDistance(
                                               point, canonical_geography_point(loop->vertex(j)),
                                               canonical_geography_point(loop->vertex(j + 1)),
                                               conversion_error) <= 0;
            }
        }
        interior = !boundary && polygon->Contains(point);
    }
    if (name == "st_intersects") {
        return interior || boundary;
    }
    if (name == "st_disjoint") {
        return !interior && !boundary;
    }
    if (name == "st_touches") {
        return boundary;
    }
    return interior;
}

static Status geography_relation(std::string_view name, const GeoShape& lhs, const GeoShape& rhs,
                                 StringRef left_wkb, StringRef right_wkb, bool* result) {
    const auto& container = name == "st_within" ? rhs : lhs;
    const auto& value = name == "st_within" ? lhs : rhs;
    const bool containment = name == "st_contains" || name == "st_within";
    // A point cannot contain a line or polygon; this is a defined false result.
    if (containment && container.type() == GeoShapeType::GEO_SHAPE_POINT &&
        value.type() != GeoShapeType::GEO_SHAPE_POINT) {
        *result = false;
        return Status::OK();
    }
    if (name == "st_equals" && lhs.type() != rhs.type()) {
        *result = false;
        return Status::OK();
    }
    if (name == "st_overlaps" &&
        (lhs.type() != rhs.type() || lhs.type() == GeoShapeType::GEO_SHAPE_POINT)) {
        *result = false;
        return Status::OK();
    }
    if (lhs.type() == GeoShapeType::GEO_SHAPE_POINT ||
        rhs.type() == GeoShapeType::GEO_SHAPE_POINT) {
        // Point equality has the same canonical position test as point containment.
        *result = geography_point_relation(name, lhs, rhs);
        return Status::OK();
    }
    MutableS2ShapeIndex left;
    MutableS2ShapeIndex right;
    const GeographyEdgeNodes nodes(left_wkb, right_wkb);
    geography_shape_index(container, nodes, &left);
    geography_shape_index(value, nodes, &right);
    S2BooleanOperation::Options options;
    options.set_polygon_model(S2BooleanOperation::PolygonModel::CLOSED);
    options.set_polyline_model(S2BooleanOperation::PolylineModel::CLOSED);
    options.set_polyline_loops_have_boundaries(false);
    if (name == "st_equals") {
        *result = S2BooleanOperation::Equals(left, right, options);
        return Status::OK();
    }
    if (containment && container.type() == GeoShapeType::GEO_SHAPE_LINE_STRING &&
        value.type() == GeoShapeType::GEO_SHAPE_POLYGON) {
        *result = false;
        return Status::OK();
    }
    if (containment && !S2BooleanOperation::Contains(left, right, options)) {
        *result = false;
        return Status::OK();
    }
    auto interior_options = options;
    interior_options.set_polygon_model(S2BooleanOperation::PolygonModel::OPEN);
    interior_options.set_polyline_model(S2BooleanOperation::PolylineModel::OPEN);
    if (containment) {
        // CLOSED Contains is covers: a polygon must not contain a line solely on its boundary.
        *result = S2BooleanOperation::Intersects(left, right, interior_options);
        return Status::OK();
    }
    if (name == "st_overlaps") {
        // Neither equality nor containment is a partial overlap.
        if (S2BooleanOperation::Contains(left, right, options) ||
            S2BooleanOperation::Contains(right, left, options)) {
            *result = false;
            return Status::OK();
        }
        if (lhs.type() == GeoShapeType::GEO_SHAPE_POLYGON) {
            *result = S2BooleanOperation::Intersects(left, right, interior_options);
            return Status::OK();
        }
        // S2 emits a crossing point as a degenerate polyline: only a real shared edge overlaps.
        std::vector<std::unique_ptr<S2Polyline>> intersection;
        S2BooleanOperation operation(
                S2BooleanOperation::OpType::INTERSECTION,
                std::make_unique<s2builderutil::S2PolylineVectorLayer>(&intersection),
                interior_options);
        S2Error error;
        if (!operation.Build(left, right, &error)) {
            return Status::InternalError("Geography intersection failed: {}", error.text());
        }
        *result = std::ranges::any_of(intersection, [](const auto& line) {
            for (int i = 1; i < line->num_vertices(); ++i) {
                if (line->vertex(i - 1) != line->vertex(i)) {
                    return true;
                }
            }
            return false;
        });
        return Status::OK();
    }
    bool intersects = S2BooleanOperation::Intersects(left, right, options);
    // A nearby disjoint curve must not become a boundary contact through the legacy
    // conversion-distance fallback used below by Intersects/Disjoint.
    if (name == "st_touches") {
        *result = intersects && !S2BooleanOperation::Intersects(left, right, interior_options);
        return Status::OK();
    }
    // Independently converted LINESTRING(10 45,10 46) and (10 45.3,10 45.9)
    // are not exactly coplanar either. Apply the same conversion bound to all
    // admitted nonpoint pairs, retaining polygon interiors and real open-line edges.
    if (!intersects) {
        S2ClosestEdgeQuery query(&left);
        S2ClosestEdgeQuery::ShapeIndexTarget target(&right);
        intersects = query.IsConservativeDistanceLessOrEqual(&target, geography_conversion_error());
    }
    DCHECK(name == "st_intersects" || name == "st_disjoint");
    *result = name == "st_intersects" ? intersects : !intersects;
    return Status::OK();
}

static std::pair<double, double> geography_point_coordinate(StringRef wkb) {
    double longitude;
    double latitude;
    const auto status = WkbParse::point_coordinates(wkb.data, wkb.size, &longitude, &latitude);
    DCHECK_EQ(status, GeoParseStatus::GEO_PARSE_OK);
    // Use original coordinates: reconstructing a shared longitude from two S2 vectors
    // can turn due north into a tiny negative bearing that wraps to almost 2*pi.
    if (std::abs(latitude) == 90) {
        longitude = 0;
    } else if (longitude == 180) {
        longitude = -180;
    }
    return {longitude, latitude};
}

static bool geography_bearing(StringRef from, StringRef to, double* bearing) {
    const auto start = geography_point_coordinate(from);
    const auto end = geography_point_coordinate(to);
    // Coincident or antipodal positions have no unique initial bearing. Compare original degrees:
    // haversine distance rounds (0, 0) -> (179.999999, 0) to pi despite its unique east bearing.
    const bool antipodal =
            start.second == -end.second &&
            (std::abs(start.second) == 90 || std::abs(start.first - end.first) == 180);
    if (start == end || antipodal) {
        return false;
    }
    *bearing = S2Earth::GetInitialBearing(S2LatLng::FromDegrees(start.second, start.first),
                                          S2LatLng::FromDegrees(end.second, end.first))
                       .radians();
    *bearing = std::fmod(*bearing + 2 * M_PI, 2 * M_PI);
    return true;
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

static bool has_spatial_leaf(const DataTypePtr& type) {
    const auto& nested = remove_nullable(type);
    switch (nested->get_primitive_type()) {
    case TYPE_GEOMETRY:
    case TYPE_GEOGRAPHY:
        return true;
    case TYPE_ARRAY:
        return has_spatial_leaf(assert_cast<const DataTypeArray&>(*nested).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*nested);
        return has_spatial_leaf(map.get_key_type()) || has_spatial_leaf(map.get_value_type());
    }
    case TYPE_STRUCT:
        for (const auto& element : assert_cast<const DataTypeStruct&>(*nested).get_elements()) {
            if (has_spatial_leaf(element)) {
                return true;
            }
        }
        return false;
    default:
        return false;
    }
}

static Status validate_spatial_range(const IColumn& input, const DataTypePtr& type, size_t start,
                                     size_t end) {
    if (!has_spatial_leaf(type) || start == end) {
        return Status::OK();
    }
    if (const auto* constant = check_and_get_column<ColumnConst>(input)) {
        return validate_spatial_range(constant->get_data_column(), type, 0, 1);
    }
    const auto* nullable = check_and_get_column<ColumnNullable>(input);
    const IColumn& column = nullable ? nullable->get_nested_column() : input;
    const auto& nested_type = remove_nullable(type);
    for (size_t row = start; row < end; ++row) {
        // NULL parents can contain default or stale child payloads; only visible values are WKB.
        if (nullable && nullable->is_null_at(row)) {
            continue;
        }
        switch (nested_type->get_primitive_type()) {
        case TYPE_GEOMETRY:
        case TYPE_GEOGRAPHY: {
            const auto value = column.get_data_at(row);
            const auto& spatial = assert_cast<const DataTypeSpatial&>(*nested_type);
            RETURN_IF_ERROR(WkbParse::validate_spatial_wkb(
                    value.data, value.size, spatial.requires_geographic_coordinates(), row));
            break;
        }
        case TYPE_ARRAY: {
            const auto& array = assert_cast<const ColumnArray&>(column);
            const auto& array_type = assert_cast<const DataTypeArray&>(*nested_type);
            RETURN_IF_ERROR(validate_spatial_range(array.get_data(), array_type.get_nested_type(),
                                                   array.offset_at(row), array.get_offsets()[row]));
            break;
        }
        case TYPE_MAP: {
            const auto& map = assert_cast<const ColumnMap&>(column);
            const auto& map_type = assert_cast<const DataTypeMap&>(*nested_type);
            RETURN_IF_ERROR(validate_spatial_range(map.get_keys(), map_type.get_key_type(),
                                                   map.offset_at(row), map.get_offsets()[row]));
            RETURN_IF_ERROR(validate_spatial_range(map.get_values(), map_type.get_value_type(),
                                                   map.offset_at(row), map.get_offsets()[row]));
            break;
        }
        case TYPE_STRUCT: {
            const auto& structure = assert_cast<const ColumnStruct&>(column);
            const auto& elements = assert_cast<const DataTypeStruct&>(*nested_type).get_elements();
            for (size_t i = 0; i < elements.size(); ++i) {
                RETURN_IF_ERROR(
                        validate_spatial_range(structure.get_column(i), elements[i], row, row + 1));
            }
            break;
        }
        default:
            break;
        }
    }
    return Status::OK();
}

Status validate_spatial_wkb_inputs(const Block& block, const ColumnNumbers& arguments) {
    for (const auto argument : arguments) {
        const auto& entry = block.get_by_position(argument);
        RETURN_IF_ERROR(validate_spatial_range(*entry.column, entry.type, 0, entry.column->size()));
    }
    return Status::OK();
}

Status append_spatial_wkb(const ColumnPtr& source, IColumn& destination,
                          const DataTypePtr& spatial_type, const NullMap::value_type* null_map) {
    auto validation_column = source;
    auto validation_type = remove_nullable(spatial_type);
    if (null_map != nullptr) {
        auto null_column = ColumnUInt8::create();
        null_column->get_data().assign(null_map, null_map + source->size());
        validation_column = ColumnNullable::create(source, ColumnPtr(std::move(null_column)));
        validation_type = make_nullable(validation_type);
    } else if (source->is_nullable()) {
        validation_type = make_nullable(validation_type);
    }
    Block validation_block;
    validation_block.insert({validation_column, validation_type, "spatial_wkb"});
    RETURN_IF_ERROR(validate_spatial_wkb_inputs(validation_block, {0}));
    for (size_t row = 0; row < validation_column->size(); ++row) {
        if (validation_column->is_null_at(row)) {
            destination.insert_default();
        } else {
            const auto value = validation_column->get_data_at(row);
            destination.insert_data(value.data, value.size);
        }
    }
    return Status::OK();
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

        GeoPoint point;
        for (size_t row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            if (is_spatial_type(input_type)) {
                double x;
                double y;
                if (WkbParse::point_coordinates(point_value.data, point_value.size, &x, &y) ==
                    GeoParseStatus::GEO_PARSE_OK) {
                    res->insert_value(x);
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
        const auto& input_type = block.get_data_type(arguments[0]);

        auto size = input->size();

        auto res = ColumnFloat64::create();
        res->reserve(size);
        auto null_map = ColumnUInt8::create(size, 0);
        auto& null_map_data = null_map->get_data();

        GeoPoint point;
        for (size_t row = 0; row < size; ++row) {
            auto point_value = input->get_data_at(row);
            if (is_spatial_type(input_type)) {
                double x;
                double y;
                if (WkbParse::point_coordinates(point_value.data, point_value.size, &x, &y) ==
                    GeoParseStatus::GEO_PARSE_OK) {
                    res->insert_value(y);
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

static double geography_distance(const GeoShape& left_shape, const GeoShape& right_shape,
                                 StringRef left_wkb, StringRef right_wkb) {
    // Keep the common point-pair path allocation-free while canonicalizing date-line/pole aliases.
    if (left_shape.type() == GeoShapeType::GEO_SHAPE_POINT &&
        right_shape.type() == GeoShapeType::GEO_SHAPE_POINT) {
        return S2Earth::GetDistanceMeters(
                canonical_geography_point(*assert_cast<const GeoPoint&>(left_shape).point()),
                canonical_geography_point(*assert_cast<const GeoPoint&>(right_shape).point()));
    }
    const GeographyEdgeNodes nodes(left_wkb, right_wkb);
    MutableS2ShapeIndex left;
    MutableS2ShapeIndex right;
    geography_shape_index(left_shape, nodes, &left);
    geography_shape_index(right_shape, nodes, &right);
    S2ClosestEdgeQuery query(&left);
    S2ClosestEdgeQuery::ShapeIndexTarget target(&right);
    return S2Earth::ToMeters(query.GetDistance(&target).ToAngle());
}

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
