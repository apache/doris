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
#include <s2/s2contains_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2lax_polygon_shape.h>
#include <s2/s2loop.h>
#include <s2/s2loop_measures.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/s2point_vector_shape.h>
#include <s2/s2pointutil.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/s2predicates.h>
#include <s2/s2shape_measures.h>

#include <algorithm>
#include <array>
#include <bit>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <numbers>
#include <optional>
#include <set>
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

static std::unique_ptr<GeoShape> decode_geography_calculation_shape(StringRef value);

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

// Error-free TwoSum preserves tiny differences from a half-turn (e.g. -1e-15 + 180).
static std::pair<double, double> geography_degree_sum(double a, double b) {
    const double high = a + b;
    const double offset = high - a;
    return {high, (a - (high - offset)) + (b - offset)};
}

// Reduce the exact degree sum before converting to radians. In particular,
// 180 + (-1e-300) must retain its small sine instead of becoming sin(pi).
static std::pair<long double, long double> geography_degree_sincos(double a, double b = 0,
                                                                   bool half = false) {
    const auto [high, low] = geography_degree_sum(a, b);
    int quadrant;
    const long double reduced =
            std::remquo(static_cast<long double>(high), half ? 180.0L : 90.0L, &quadrant) +
            static_cast<long double>(low);
    const auto radians = reduced * (std::numbers::pi_v<long double> / 180) / (half ? 2 : 1);
    const auto sine = std::sin(radians);
    const auto cosine = std::cos(radians);
    switch ((quadrant % 4 + 4) % 4) {
    case 0:
        return {sine, cosine};
    case 1:
        return {cosine, -sine};
    case 2:
        return {-sine, -cosine};
    default:
        return {-cosine, sine};
    }
}

// Unlike cross(ToPoint(a), ToPoint(b)), these product identities do not subtract
// rounded nearly opposite vectors. Longitude-midpoint coordinates give
// (-sin(dLon/2)*sin(latA+latB), cos(dLon/2)*sin(latA-latB),
//  cos(latA)*cos(latB)*sin(dLon)) for the oriented normal.
struct GeographyRawArc {
    S2Point normal;
    S2Point midpoint;

    GeographyRawArc(const GeoCoordinate& a, const GeoCoordinate& b) {
        // Wide exponents retain products of the smallest binary64 degree residuals.
        static_assert(std::numeric_limits<long double>::digits >= 64 &&
                      std::numeric_limits<long double>::min_exponent <= -4096);
        const auto [sd, cd] = geography_degree_sincos(b.x, -a.x, true);
        const auto [sm, cm] = geography_degree_sincos(a.x, b.x, true);
        const auto rotate_unit = [&](long double x, long double y, long double z) {
            const auto norm = std::sqrt(x * x + y * y + z * z);
            DCHECK_GT(norm, 0);
            return S2Point(static_cast<double>((cm * x - sm * y) / norm),
                           static_cast<double>((sm * x + cm * y) / norm),
                           static_cast<double>(z / norm));
        };
        normal = rotate_unit(-sd * geography_degree_sincos(a.y, b.y).first,
                             cd * geography_degree_sincos(a.y, -b.y).first,
                             geography_degree_sincos(a.y).second *
                                     geography_degree_sincos(b.y).second *
                                     geography_degree_sincos(b.x, -a.x).first);
        const auto [sl, cl] = geography_degree_sincos(a.y, b.y, true);
        const auto [st, ct] = geography_degree_sincos(b.y, -a.y, true);
        midpoint = rotate_unit(cd * cl * ct, -sd * sl * st, sl * ct);
    }
};

// Raw identity must survive ToPoint rounding: 58 and 58.00000000000001 degrees
// produce the same S2Point, but only one is an endpoint of the original edge.
struct GeographyVertex {
    GeoCoordinate coordinate;
    S2Point point;

    GeographyVertex() : GeographyVertex({.x = 0, .y = 0}) {}
    explicit GeographyVertex(GeoCoordinate value) : coordinate(value) {
        if (std::abs(coordinate.y) == 90) {
            coordinate.x = 0;
            point = {0, 0, coordinate.y > 0 ? 1.0 : -1.0};
        } else {
            if (coordinate.x == 180) {
                coordinate.x = -180;
            }
            point = S2LatLng::FromDegrees(coordinate.y, coordinate.x).ToPoint();
        }
    }
    bool operator==(const GeographyVertex& other) const {
        return coordinate.x == other.coordinate.x && coordinate.y == other.coordinate.y;
    }
    bool operator<(const GeographyVertex& other) const {
        return std::pair {coordinate.x, coordinate.y} <
               std::pair {other.coordinate.x, other.coordinate.y};
    }
};

static bool geography_antipodal(const GeographyVertex& a, const GeographyVertex& b) {
    return a.coordinate.y == -b.coordinate.y &&
           (std::abs(a.coordinate.y) == 90 ||
            geography_degree_sum(std::max(a.coordinate.x, b.coordinate.x),
                                 -std::min(a.coordinate.x, b.coordinate.x)) ==
                    std::pair {180.0, 0.0});
}

struct GeographyCoordinates {
    GeoShapeType type = GeoShapeType::GEO_SHAPE_ANY;
    DorisVector<DorisVector<GeographyVertex>> sequences;
    GeoParseStatus status;

    explicit GeographyCoordinates(StringRef wkb) {
        GeoCoordinateListList raw;
        status = WkbParse::calculation_coordinates(wkb.data, wkb.size, &raw, &type);
        if (status != GeoParseStatus::GEO_PARSE_OK) {
            return;
        }
        for (const auto& sequence : raw.list) {
            auto& vertices = sequences.emplace_back();
            for (const auto& coordinate : sequence->list) {
                vertices.emplace_back(coordinate);
            }
        }
    }
};

static std::unique_ptr<GeoShape> decode_geography_calculation_shape(StringRef value) {
    GeographyCoordinates raw(value);
    if (raw.status != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }
    if (raw.type != GeoShapeType::GEO_SHAPE_POLYGON) {
        GeoParseStatus status;
        return GeoShape::from_wkb_bytes(value.data, value.size, status);
    }
    std::vector<std::unique_ptr<S2Loop>> loops;
    std::vector<const S2Loop*> input_loops;
    for (const auto& ring : raw.sequences) {
        std::vector<S2Point> points;
        for (size_t i = 0; i + 1 < ring.size(); ++i) {
            const auto& a = ring[i];
            const auto& b = ring[i + 1];
            // (0,0)->(180,0) has no unique arc and would normalize a zero raw normal.
            if (geography_antipodal(a, b)) {
                return nullptr;
            }
            points.push_back(a.point);
            // Stabilize before IsValid/Contains, not after a valid near-edge hole
            // has been rejected by the rounded (-45+e,-45)->(135-e,45) shell.
            if (a.point.DotProd(b.point) < 0) {
                points.push_back(GeographyRawArc(a.coordinate, b.coordinate).midpoint);
            }
        }
        points.erase(std::ranges::unique(points).begin(), points.end());
        if (points.size() > 1 && points.front() == points.back()) {
            points.pop_back();
        }
        if (points.size() < 3) {
            return nullptr;
        }
        auto loop = std::make_unique<S2Loop>(points, S2Debug::DISABLE);
        if (!loop->IsValid()) {
            return nullptr;
        }
        loop->Normalize();
        if (!loops.empty() && !loops.front()->Contains(*loop)) {
            return nullptr;
        }
        input_loops.push_back(loop.get());
        loops.push_back(std::move(loop));
    }
    auto polygon = std::make_unique<S2Polygon>(std::move(loops), S2Debug::DISABLE);
    if (!polygon->IsValid()) {
        return nullptr;
    }
    std::vector<int> depths;
    for (const auto* loop : input_loops) {
        depths.push_back(loop->depth());
    }
    return GeoPolygon::create_unique(std::move(polygon), std::move(depths));
}

#ifdef BE_TEST
static thread_local size_t geography_exact_proofs = 0;
static thread_local size_t geography_candidate_visits = 0;
#endif

// Test coplanarity of the original degree ordinates, not independently rounded S2 vectors.
// For example (45,-45), (90,0), (135,45) share an arc although ToPoint's determinant is nonzero.
static bool geography_common_great_circle(const GeoCoordinate& a, const GeoCoordinate& b,
                                          const GeoCoordinate& c) {
#ifdef BE_TEST
    ++geography_exact_proofs;
#endif
    using boost::multiprecision::cpp_int;
    const std::array<double, 6> ordinates {a.y, a.x, b.y, b.x, c.y, c.x};
    std::array<uint64_t, 6> significands;
    std::array<int, 6> exponents;
    int denominator_bits = 0;
    for (size_t i = 0; i < ordinates.size(); ++i) {
        const auto bits = std::bit_cast<uint64_t>(ordinates[i]);
        const int biased = (bits >> 52) & 0x7ff;
        uint64_t significand = bits & ((uint64_t {1} << 52) - 1);
        int exponent = biased ? biased - 1075 : -1074;
        if (biased) {
            significand |= uint64_t {1} << 52;
        }
        if (significand != 0) {
            const int zeros = std::countr_zero(significand);
            significand >>= zeros;
            exponent += zeros;
            denominator_bits = std::max(denominator_bits, -exponent);
        } else {
            exponent = 0;
        }
        significands[i] = significand;
        exponents[i] = exponent;
    }
    std::array<cpp_int, 6> angles;
    for (size_t i = 0; i < angles.size(); ++i) {
        angles[i] = cpp_int(significands[i]) << (denominator_bits + exponents[i]);
        if (std::signbit(ordinates[i])) {
            angles[i] = -angles[i];
        }
    }
    // Binary-rational degrees have denominator 2^q. Exp(i*angle) is a power of a
    // root of unity of order 360*2^q. Its minimal polynomial is Phi_90(t), with
    // t = z^(2^(q+2)), and Phi_90(t) = t^24+t^21-t^15-t^12-t^9+t^3+1.
    // Expand cos(lat_i)cos(lat_j)sin(lat_k)sin(lon_j-lon_i), cyclically, into
    // 48 integer-coefficient powers. Equal residues modulo 2^(q+2) share one
    // polynomial; reduction by Phi_90 proves zero exactly, including tiny nonzero controls.
    const int shift = denominator_bits + 2;
    const cpp_int basis = cpp_int(1) << shift;
    const cpp_int period = 90 * basis;
    std::map<cpp_int, std::array<int, 90>> polynomials;
    for (int i = 0; i < 3; ++i) {
        const int j = (i + 1) % 3;
        const int k = (i + 2) % 3;
        for (int mask = 0; mask < 16; ++mask) {
            std::array<int, 4> signs;
            for (int bit = 0; bit < 4; ++bit) {
                signs[bit] = (mask & (1 << bit)) ? 1 : -1;
            }
            cpp_int power = signs[0] * angles[2 * i] + signs[1] * angles[2 * j] +
                            signs[2] * angles[2 * k] +
                            signs[3] * (angles[2 * j + 1] - angles[2 * i + 1]);
            power %= period;
            if (power < 0) {
                power += period;
            }
            const int quotient = (power >> shift).convert_to<int>();
            const cpp_int residue = power - (cpp_int(quotient) << shift);
            polynomials[residue][quotient] += signs[2] * signs[3];
        }
    }
    for (auto& [residue, coefficients] : polynomials) {
        for (int i = 89; i >= 24; --i) {
            const int coefficient = coefficients[i];
            coefficients[i] = 0;
            coefficients[i - 3] -= coefficient;
            coefficients[i - 9] += coefficient;
            coefficients[i - 12] += coefficient;
            coefficients[i - 15] += coefficient;
            coefficients[i - 21] -= coefficient;
            coefficients[i - 24] -= coefficient;
        }
        if (std::ranges::any_of(coefficients, [](int value) { return value != 0; })) {
            return false;
        }
    }
    return true;
}

// Keep each atomic segment in its own shape. Pinned S2 ProcessEdge1 uses a chain's
// global start as a local offset in one endpoint path, so multiple chains per line
// shape are unsafe. Closed AA shapes restore internal vertices in the OPEN model.
class GeographyLineShape final : public S2Shape {
public:
    GeographyLineShape(const S2Point& a, const S2Point& b) { _edge.emplace_back(a, b); }
    int num_edges() const override { return 1; }
    Edge edge(int i) const override {
        DCHECK_EQ(i, 0);
        return _edge.front();
    }
    int dimension() const override { return 1; }
    ReferencePoint GetReferencePoint() const override { return ReferencePoint::Contained(false); }
    int num_chains() const override { return 1; }
    Chain chain(int i) const override {
        DCHECK_EQ(i, 0);
        return {0, 1};
    }
    Edge chain_edge(int i, int j) const override {
        DCHECK_EQ(i, 0);
        return edge(j);
    }
    ChainPosition chain_position(int i) const override {
        DCHECK_EQ(i, 0);
        return {0, 0};
    }

private:
    // Owning storage is checked/accounted by the current query's allocator.
    DorisVector<Edge> _edge;
};

class GeographyEdgeNodes {
public:
    GeographyCoordinates left;
    GeographyCoordinates right;

    GeographyEdgeNodes(StringRef left_wkb, StringRef right_wkb) : left(left_wkb), right(right_wkb) {
        DCHECK_EQ(left.status, GeoParseStatus::GEO_PARSE_OK);
        DCHECK_EQ(right.status, GeoParseStatus::GEO_PARSE_OK);
        DorisMap<double, DorisVector<GeographyVertex>> longitude_vertices;
        std::set<GeographyVertex, std::less<>, CustomStdAllocator<GeographyVertex>> coordinates;
        for (const auto* shape : {&left, &right}) {
            for (const auto& sequence : shape->sequences) {
                for (const auto& point : sequence) {
                    if (!coordinates.insert(point).second) {
                        continue;
                    }
                    const auto& coordinate = point.coordinate;
                    longitude_vertices[coordinate.x].push_back(point);
                    if (std::abs(coordinate.y) != 90) {
                        _meridians[meridian(coordinate.x)][coordinate.x < 0].push_back(
                                {coordinate.y, point});
                    }
                }
            }
        }
        // Keep dense longitude groups separately so a circle can skip its known
        // endpoint longitudes before enumerating nearby points. At sqrt(N) points
        // per group there are at most sqrt(N) extra indexes, even with unrelated
        // vertices padding a near-antipodal spoke input.
        const auto dense_size =
                std::max(size_t {2}, static_cast<size_t>(std::ceil(std::sqrt(coordinates.size()))));
        for (const auto& [longitude, points] : longitude_vertices) {
            auto& index =
                    points.size() >= dense_size ? _dense_longitudes[longitude] : _vertex_index;
            for (const auto& point : points) {
                index.Add(point.point, point);
            }
        }
        for (auto& entry : _meridians) {
            for (auto& side : entry.second) {
                side.push_back({-90, GeographyVertex({.x = 0, .y = -90})});
                side.push_back({90, GeographyVertex({.x = 0, .y = 90})});
                std::ranges::sort(side, {}, &Vertex::latitude);
            }
        }
    }

    bool contains_point(const GeographyVertex& start, const GeographyVertex& end,
                        const GeographyVertex& point) const {
        // Preserve exact endpoints, including date-line and pole aliases.
        if (point == start || point == end) {
            return true;
        }
        if (start == end) {
            return false;
        }
        const auto& a = start.coordinate;
        const auto& b = end.coordinate;
        const auto& c = point.coordinate;
        const auto key = meridian(std::abs(a.y) == 90 ? b.x : a.x);
        bool member = false;
        if (meridian_edge(start, end, [&](const auto& vertices, double from, double to) {
                // Opposite-longitude edges can pass through a pole; the pole belongs
                // to both legs, whereas other positions must be on the selected side.
                const bool same_side =
                        std::abs(c.y) == 90 ||
                        (meridian(c.x) == key && &_meridians.at(key)[c.x < 0] == &vertices);
                member |= same_side && c.y >= std::min(from, to) && c.y <= std::max(from, to);
            })) {
            return member;
        }
        const bool wraps = geography_degree_sum(std::max(a.x, b.x), -std::min(a.x, b.x)) >
                           std::pair {180.0, 0.0};
        const bool within = wraps ? (c.x < std::min(a.x, b.x) || c.x > std::max(a.x, b.x))
                                  : (c.x > std::min(a.x, b.x) && c.x < std::max(a.x, b.x));
        // Near-antipodal endpoint conversion can rotate the S2 plane substantially.
        // POINT(45 0) is on (-44.99999999999997 -45,134.99999999999997 45);
        // decide from raw degrees rather than accepting a larger distance tolerance.
        return within && geography_common_great_circle(a, b, c);
    }

    void append_edge(const GeographyVertex& start, const GeographyVertex& end,
                     DorisVector<GeographyVertex>* output) const {
        output->push_back(start);
        if (start == end) {
            return;
        }
        if (meridian_edge(start, end, [&](const auto& vertices, double from, double to) {
                append_range(vertices, from, to, output);
                if (std::abs(to) == 90) {
                    const GeographyVertex pole({.x = 0, .y = to});
                    if (end != pole) {
                        output->push_back(pole);
                    }
                }
            })) {
            return;
        }
        const auto& a = start.coordinate;
        const auto& b = end.coordinate;
        const bool wraps = geography_degree_sum(std::max(a.x, b.x), -std::min(a.x, b.x)) >
                           std::pair {180.0, 0.0};
        const bool east = (a.x < b.x) != wraps;
        // A near-antipodal rounded edge can miss true raw-coordinate nodes. Two
        // stable half-arcs recall those nodes without a hemispherical search.
        const auto midpoint = GeographyRawArc(a, b).midpoint;
        DorisVector<GeographyVertex> on_edge;
        for (const auto& endpoint : {start, end}) {
            visit_edge_candidates(
                    endpoint.point, midpoint, [](double) { return false; },
                    [&](const GeographyVertex& point) {
                        const auto& c = point.coordinate;
                        // Longitude is strictly monotone along every non-meridian minor arc.
                        // Check the original interval (including date-line wrap), not a distance
                        // tolerance that could append a point just beyond an endpoint.
                        const bool within =
                                wraps ? (c.x < std::min(a.x, b.x) || c.x > std::max(a.x, b.x))
                                      : (c.x > std::min(a.x, b.x) && c.x < std::max(a.x, b.x));
                        if (within && geography_common_great_circle(a, b, c)) {
                            on_edge.push_back(point);
                        }
                    });
        }
        std::ranges::sort(on_edge, [&](const GeographyVertex& x, const GeographyVertex& y) {
            const double x_lon = x.coordinate.x;
            const double y_lon = y.coordinate.x;
            if ((x_lon < a.x) != (y_lon < a.x)) {
                return east ? x_lon >= a.x : x_lon < a.x;
            }
            return east ? x_lon < y_lon : x_lon > y_lon;
        });
        on_edge.erase(std::ranges::unique(on_edge).begin(), on_edge.end());
        output->insert(output->end(), on_edge.begin(), on_edge.end());
    }

    // Calculation-only points never enter the raw-degree identity/noding map.
    std::optional<S2Point> arc_midpoint(const GeographyVertex& a, const GeographyVertex& b) const {
        if (a.point.DotProd(b.point) < 0) {
            return GeographyRawArc(a.coordinate, b.coordinate).midpoint;
        }
        return std::nullopt;
    }

    void add_line(const GeographyCoordinates& shape, MutableS2ShapeIndex* index) const {
        using Edge = std::pair<GeographyVertex, GeographyVertex>;
        using EdgeSet = std::set<Edge, std::less<>, CustomStdAllocator<Edge>>;
        using Interval = std::pair<double, double>;
        DorisMap<const DorisVector<Vertex>*, DorisVector<Interval>> intervals;
        EdgeSet raw_edges;
        const auto& line = shape.sequences.front();
        const auto& first = line.front();
        const auto& last = line.back();
        for (size_t i = 0; i + 1 < line.size(); ++i) {
            const auto& a = line[i];
            const auto& b = line[i + 1];
            if (a == b) {
                continue;
            }
            if (!meridian_edge(a, b, [&](const auto& vertices, double from, double to) {
                    intervals[&vertices].emplace_back(std::min(from, to), std::max(from, to));
                })) {
                raw_edges.emplace(std::min(a, b), std::max(a, b));
            }
        }
        EdgeSet edges;
        auto add_edge = [&](const GeographyVertex& a, const GeographyVertex& b) {
            edges.emplace(std::min(a, b), std::max(a, b));
        };
        // Merge latitude coverage before splitting: N repeated or overlapping edges
        // and M collinear vertices produce O(N+M) scratch, never an N*M vertex walk.
        for (auto& [vertices, ranges] : intervals) {
            std::ranges::sort(ranges);
            auto emit = [&](double from, double to) {
                auto begin = std::ranges::lower_bound(*vertices, from, {}, &Vertex::latitude);
                const auto end = std::ranges::upper_bound(*vertices, to, {}, &Vertex::latitude);
                for (auto next = begin + 1; next != end; ++begin, ++next) {
                    add_edge(begin->point, next->point);
                }
            };
            double from = ranges.front().first;
            double to = ranges.front().second;
            for (const auto& range : ranges) {
                if (range.first > to) {
                    emit(from, to);
                    from = range.first;
                }
                to = std::max(to, range.second);
            }
            emit(from, to);
        }
        add_circle_edges(raw_edges, add_edge);
        auto add = [&](const S2Point& a, const S2Point& b) {
            index->Add(std::make_unique<GeographyLineShape>(a, b));
        };
        if (edges.empty()) {
            // A raw pole-alias line may collapse to one canonical calculation position.
            add(first.point, first.point);
        }
        std::set<S2Point, std::less<>, CustomStdAllocator<S2Point>> interior_vertices;
        for (const auto& [a, b] : edges) {
            const auto midpoint = arc_midpoint(a, b);
            if (midpoint) {
                add(a.point, *midpoint);
                add(*midpoint, b.point);
                // The synthetic split is interior, not a new OPEN line boundary:
                // a crossing at this point must not become ST_Touches=true.
                interior_vertices.insert(*midpoint);
            } else {
                add(a.point, b.point);
            }
            interior_vertices.insert(a.point);
            interior_vertices.insert(b.point);
        }
        for (const auto& point : interior_vertices) {
            if (first == last || (point != first.point && point != last.point)) {
                add(point, point);
            }
        }
    }

private:
    struct Vertex {
        double latitude;
        GeographyVertex point;
    };
    struct Circle {
        GeographyVertex a;
        GeographyVertex b;
        S2Point normal;
        DorisVector<std::pair<double, double>> ranges;
        DorisMap<double, GeographyVertex> vertices;
    };

    template <typename Edges, typename AddEdge>
    void add_circle_edges(const Edges& edges, AddEdge&& add_edge) const {
        DorisVector<Circle> circles;
        S2PointIndex<size_t> normal_index;
        const double rounding = 64 * std::numeric_limits<double>::epsilon();
        for (const auto& [a, b] : edges) {
            const auto normal = GeographyRawArc(a.coordinate, b.coordinate).normal;
            size_t group = circles.size();
            S2ClosestPointQuery<size_t> query(&normal_index);
            // Raw trigonometric products avoid the epsilon/sin(length) error
            // amplification. Normals only recall candidates; exact proof decides.
            query.mutable_options()->set_conservative_max_distance(S1Angle::Radians(rounding));
            S2ClosestPointQuery<size_t>::PointTarget target(normal);
            for (const auto& candidate : query.FindClosestPoints(&target)) {
#ifdef BE_TEST
                ++geography_candidate_visits;
#endif
                const auto& circle = circles[candidate.data()];
                const auto& x = circle.a.coordinate;
                const auto& y = circle.b.coordinate;
                const auto on_circle = [&](const GeographyVertex& point) {
                    return point == circle.a || point == circle.b ||
                           geography_common_great_circle(x, y, point.coordinate);
                };
                if (on_circle(a) && on_circle(b)) {
                    group = candidate.data();
                    break;
                }
            }
            if (group == circles.size()) {
                normal_index.Add(normal, group);
                normal_index.Add(-normal, group);
                circles.push_back({a, b, normal, {}, {}});
            }
            auto& circle = circles[group];
            const double lo = std::min(a.coordinate.x, b.coordinate.x);
            const double hi = std::max(a.coordinate.x, b.coordinate.x);
            // Longitude cut points are interval sentinels, not fabricated spatial
            // vertices. For 170->-170 the real wrap edge is emitted below.
            if (geography_degree_sum(hi, -lo) > std::pair {180.0, 0.0}) {
                circle.ranges.emplace_back(-180, lo);
                circle.ranges.emplace_back(hi, 180);
            } else {
                circle.ranges.emplace_back(lo, hi);
            }
            // Keep every raw endpoint, especially original open-line endpoints
            // inside a larger merged interval: OPEN must still exclude those points.
            circle.vertices.emplace(a.coordinate.x, a);
            circle.vertices.emplace(b.coordinate.x, b);
        }
        for (auto& circle : circles) {
            collect_circle_vertices(circle);
            emit_circle_edges(circle, add_edge);
        }
    }

    template <typename SkipLongitude, typename Visit>
    void visit_edge_candidates(const S2Point& a, const S2Point& b, SkipLongitude&& skip_longitude,
                               Visit&& visit) const {
        const auto query_index = [&](const S2PointIndex<GeographyVertex>& index) {
            S2ClosestPointQuery<GeographyVertex> query(&index);
            query.mutable_options()->set_conservative_max_distance(
                    S1Angle::Radians(64 * std::numeric_limits<double>::epsilon()));
            S2ClosestPointQuery<GeographyVertex>::EdgeTarget target(a, b);
            for (const auto& candidate : query.FindClosestPoints(&target)) {
#ifdef BE_TEST
                ++geography_candidate_visits;
#endif
                visit(candidate.data());
            }
        };
        query_index(_vertex_index);
        for (const auto& [longitude, index] : _dense_longitudes) {
#ifdef BE_TEST
            ++geography_candidate_visits;
#endif
            // A non-meridian circle has only one nonpolar point at each longitude.
            // Dense endpoint groups must be skipped before the S2 query, including
            // when unrelated vertices make them a minority of the complete input.
            if (!skip_longitude(longitude)) {
                query_index(index);
            }
        }
    }

    void collect_circle_vertices(Circle& circle) const {
        // Query a whole circle once, using four non-antipodal quarter arcs.
        const auto u = S2::Ortho(circle.normal);
        const auto v = circle.normal.CrossProd(u).Normalize();
        const std::array<S2Point, 4> quarter {u, v, -u, -v};
        std::set<GeographyVertex, std::less<>, CustomStdAllocator<GeographyVertex>> seen;
        for (size_t i = 0; i < quarter.size(); ++i) {
            visit_edge_candidates(
                    quarter[i], quarter[(i + 1) % 4],
                    [&](double longitude) { return circle.vertices.contains(longitude); },
                    [&](const GeographyVertex& point) {
                        const auto& coordinate = point.coordinate;
                        if (seen.insert(point).second && !circle.vertices.contains(coordinate.x) &&
                            geography_common_great_circle(circle.a.coordinate, circle.b.coordinate,
                                                          coordinate)) {
                            circle.vertices.emplace(coordinate.x, point);
                        }
                    });
        }
    }

    template <typename AddEdge>
    static void emit_circle_edges(Circle& circle, AddEdge&& add_edge) {
        std::ranges::sort(circle.ranges);
        DorisVector<std::pair<double, double>> merged;
        for (const auto& range : circle.ranges) {
            if (merged.empty() || range.first > merged.back().second) {
                merged.push_back(range);
            } else {
                merged.back().second = std::max(merged.back().second, range.second);
            }
        }
        for (const auto& [lo, hi] : merged) {
            const auto begin = circle.vertices.lower_bound(lo);
            const auto end = circle.vertices.upper_bound(hi);
            // Each original interval retains an endpoint, including either side
            // of the date-line cut. Merging and node collection never remove it.
            DCHECK(begin != end);
            auto previous = begin;
            for (auto next = std::next(begin); next != end; ++previous, ++next) {
                add_edge(previous->second, next->second);
            }
        }
        if (merged.front().first == -180 && merged.back().second == 180) {
            add_edge(circle.vertices.rbegin()->second, circle.vertices.begin()->second);
        }
    }

    static std::pair<double, double> meridian(double longitude) {
        return longitude >= 0 ? std::pair {longitude, 0.0} : geography_degree_sum(longitude, 180);
    }
    template <typename Append>
    bool meridian_edge(const GeographyVertex& start, const GeographyVertex& end,
                       Append&& append) const {
        const auto& a = start.coordinate;
        const auto& b = end.coordinate;
        const bool a_pole = std::abs(a.y) == 90;
        const bool b_pole = std::abs(b.y) == 90;
        const auto key = meridian(a_pole ? b.x : a.x);
        if (!a_pole && !b_pole && key != meridian(b.x)) {
            return false;
        }
        // Exact opposite endpoints (e.g. (0,10),(180,-10)) have no unique minor arc.
        if (a.y == -b.y && ((a_pole && b_pole) || (a.x < 0) != (b.x < 0))) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Geography calculations do not support antipodal edges");
        }
        const bool a_side = (a_pole ? b.x : a.x) < 0;
        const bool b_side = (b_pole ? a.x : b.x) < 0;
        const auto& group = _meridians.at(key);
        if (a_side == b_side) {
            append(group[a_side], a.y, b.y);
        } else {
            // (10,80)->(-170,80) goes via the north pole; retain raw latitude on each side.
            const double latitude = a.y > -b.y ? 90 : -90;
            append(group[a_side], a.y, latitude);
            append(group[b_side], latitude, b.y);
        }
        return true;
    }
    static void append_range(const DorisVector<Vertex>& vertices, double from, double to,
                             DorisVector<GeographyVertex>* output) {
        const auto first =
                std::ranges::upper_bound(vertices, std::min(from, to), {}, &Vertex::latitude);
        const auto last =
                std::ranges::lower_bound(vertices, std::max(from, to), {}, &Vertex::latitude);
        if (from < to) {
            for (auto it = first; it != last; ++it) {
                output->push_back(it->point);
            }
        } else {
            auto current = last;
            while (current != first) {
                output->push_back((--current)->point);
            }
        }
    }
    DorisMap<double, S2PointIndex<GeographyVertex>> _dense_longitudes;
    S2PointIndex<GeographyVertex> _vertex_index;
    DorisMap<std::pair<double, double>, std::array<DorisVector<Vertex>, 2>> _meridians;
};

static void geography_shape_index(const GeoShape& shape, const GeographyCoordinates& raw,
                                  const GeographyEdgeNodes& nodes, MutableS2ShapeIndex* index) {
    switch (shape.type()) {
    case GeoShapeType::GEO_SHAPE_POINT:
        index->Add(std::make_unique<S2PointVectorShape>(
                std::vector<S2Point> {raw.sequences.front().front().point}));
        break;
    case GeoShapeType::GEO_SHAPE_LINE_STRING:
        nodes.add_line(raw, index);
        break;
    case GeoShapeType::GEO_SHAPE_POLYGON: {
        const auto& depths = assert_cast<const GeoPolygon&>(shape).calculation_ring_depths();
        DCHECK_EQ(depths.size(), raw.sequences.size());
        std::vector<S2LaxPolygonShape::Loop> loops;
        for (size_t i = 0; i < raw.sequences.size(); ++i) {
            auto& vertices = loops.emplace_back();
            const auto& ring = raw.sequences[i];
            DorisVector<GeographyVertex> edge;
            for (size_t j = 0; j + 1 < ring.size(); ++j) {
                edge.clear();
                nodes.append_edge(ring[j], ring[j + 1], &edge);
                edge.push_back(ring[j + 1]);
                for (size_t k = 0; k + 1 < edge.size(); ++k) {
                    vertices.push_back(edge[k].point);
                    // Use the same stable long arcs for interiors and distances.
                    // The raw (-45+e,-45)->(135-e,45) edge otherwise changes sides
                    // around POINT(45 0.000001) when its midpoint is inserted.
                    if (const auto midpoint = nodes.arc_midpoint(edge[k], edge[k + 1])) {
                        vertices.push_back(*midpoint);
                    }
                }
            }
            // A thin near-antipodal ring may have been reversed by normalization
            // of the rounded edge. Restore the smaller-region shell orientation
            // after stabilization while preserving the admitted hole depths.
            // Span normalization also retains canonical pole-alias degeneracies.
            if (S2::IsNormalized(vertices) == (depths[i] % 2 != 0)) {
                std::ranges::reverse(vertices);
            }
        }
        index->Add(std::make_unique<S2LaxPolygonShape>(loops));
        break;
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Unexpected Geography calculation shape");
    }
}

#ifdef BE_TEST
size_t geography_calculation_edges_for_test(const std::string& left, const std::string& right,
                                            size_t* exact_proofs, size_t* candidate_visits) {
    geography_exact_proofs = 0;
    geography_candidate_visits = 0;
    const auto a = decode_geography_calculation_shape(StringRef(left));
    const auto b = decode_geography_calculation_shape(StringRef(right));
    const GeographyEdgeNodes nodes {StringRef(left), StringRef(right)};
    MutableS2ShapeIndex a_index;
    MutableS2ShapeIndex b_index;
    geography_shape_index(*a, nodes.left, nodes, &a_index);
    geography_shape_index(*b, nodes.right, nodes, &b_index);
    size_t edges = 0;
    for (const auto* shape : a_index) {
        edges += shape->num_edges();
    }
    for (const auto* shape : b_index) {
        edges += shape->num_edges();
    }
    if (exact_proofs != nullptr) {
        *exact_proofs = geography_exact_proofs;
    }
    if (candidate_visits != nullptr) {
        *candidate_visits = geography_candidate_visits;
    }
    return edges;
}
#endif

static S1ChordAngle geography_conversion_error() {
    // S2LatLng::ToPoint and degrees-to-radians rounding can separate positions on
    // the same meridian. This is about 1e-8 metres, not a user snap radius.
    return S1ChordAngle(S1Angle::Radians(8 * std::numeric_limits<double>::epsilon()));
}

static bool geography_point_relation(std::string_view name, const GeoShape& lhs,
                                     const GeoShape& rhs, const GeographyEdgeNodes& nodes) {
    const auto& other = lhs.type() == GeoShapeType::GEO_SHAPE_POINT ? rhs : lhs;
    const auto& point_raw = lhs.type() == GeoShapeType::GEO_SHAPE_POINT ? nodes.left : nodes.right;
    const auto& other_raw = lhs.type() == GeoShapeType::GEO_SHAPE_POINT ? nodes.right : nodes.left;
    const auto& point = point_raw.sequences.front().front();
    bool interior = false;
    bool boundary = false;
    if (other.type() == GeoShapeType::GEO_SHAPE_POINT) {
        interior = point == other_raw.sequences.front().front();
    } else if (other.type() == GeoShapeType::GEO_SHAPE_LINE_STRING) {
        const auto& line = other_raw.sequences.front();
        bool member = false;
        for (size_t i = 0; i + 1 < line.size(); ++i) {
            member = member || nodes.contains_point(line[i], line[i + 1], point);
        }
        const auto& first = line.front();
        const auto& last = line.back();
        boundary = first != last && (point == first || point == last);
        interior = member && !boundary;
    } else {
        DCHECK(other.type() == GeoShapeType::GEO_SHAPE_POLYGON);
        for (const auto& ring : other_raw.sequences) {
            for (size_t j = 0; j + 1 < ring.size(); ++j) {
                boundary = boundary || nodes.contains_point(ring[j], ring[j + 1], point);
            }
        }
        if (!boundary) {
            MutableS2ShapeIndex index;
            geography_shape_index(other, other_raw, nodes, &index);
            interior = MakeS2ContainsPointQuery(&index).Contains(point.point);
        }
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
        const GeographyEdgeNodes nodes(left_wkb, right_wkb);
        *result = geography_point_relation(name, lhs, rhs, nodes);
        return Status::OK();
    }
    MutableS2ShapeIndex left;
    MutableS2ShapeIndex right;
    const GeographyEdgeNodes nodes(left_wkb, right_wkb);
    geography_shape_index(container, name == "st_within" ? nodes.right : nodes.left, nodes, &left);
    geography_shape_index(value, name == "st_within" ? nodes.left : nodes.right, nodes, &right);
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
    // Likewise retain the nonzero longitude residual in (1e-300, 0) -> (-180, 0).
    const bool antipodal =
            start.second == -end.second &&
            (std::abs(start.second) == 90 ||
             geography_degree_sum(std::max(start.first, end.first),
                                  -std::min(start.first, end.first)) == std::pair {180.0, 0.0});
    if (start == end || antipodal) {
        return false;
    }
    // Half-angle form avoids cancellation between nearly opposite latitude terms.
    // Reduce the original degrees before converting to radians so an exact 180-degree
    // longitude difference has zero east component even next to an antipode.
    // For example, (0, 45) -> (180, nextafter(-45, +inf)) starts due north, not west.
    const auto [sin_half, cos_half] = geography_degree_sincos(end.first, -start.first, true);
    const long double east = geography_degree_sincos(end.second).second *
                             geography_degree_sincos(end.first, -start.first).first;
    const long double north =
            geography_degree_sincos(end.second, -start.second).first * cos_half * cos_half +
            geography_degree_sincos(end.second, start.second).first * sin_half * sin_half;
    *bearing = std::atan2(east, north);
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

static bool compute_geo_area(GeoShape& shape, StringRef wkb, const DataTypePtr& input_type,
                             double* area, const std::string& unit) {
    if (is_spatial_type(input_type) && shape.type() == GeoShapeType::GEO_SHAPE_POLYGON) {
        // Use the same stable raw-coordinate edges as typed relations and distance.
        // Measuring the independently rounded GeoPolygon changes near-antipodal arcs.
        // (-45+e, -45) -> (135-e, 45) must retain its area when subdivided at (45, 0).
        GeographyEdgeNodes nodes(wkb, wkb);
        MutableS2ShapeIndex index;
        geography_shape_index(shape, nodes.left, nodes, &index);
        const double steradians = S2::GetArea(*index.shape(0));
        *area = unit == "square_meters" ? S2Earth::SteradiansToSquareMeters(steradians)
                                        : S2Earth::SteradiansToSquareKm(steradians);
        return true;
    }
    return GeoShape::ComputeArea(&shape, area, unit);
}

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
            if (!compute_geo_area(*shape, shape_value, input_type, &area, "square_meters")) {
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
            if (!compute_geo_area(*shape, shape_value, input_type, &area, "square_km")) {
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
    geography_shape_index(left_shape, nodes.left, nodes, &left);
    geography_shape_index(right_shape, nodes.right, nodes, &right);
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
