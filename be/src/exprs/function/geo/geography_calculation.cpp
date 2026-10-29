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

#include "exprs/function/geo/geography_calculation.h"

#include <glog/logging.h>
#include <s2/mutable_s2shape_index.h>
#include <s2/s2boolean_operation.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2closest_point_query.h>
#include <s2/s2contains_point_query.h>
#include <s2/s2edge_distances.h>
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
// Integer proofs and software-wide arithmetic need no quadmath wrappers. Boost's float128
// numeric_limits specialization conflicts with libstdc++ 15 in the GCC performance build.
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 15
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#endif
#include <boost/multiprecision/cpp_bin_float.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 15
#undef BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#endif
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <numbers>
#include <optional>
#include <set>
#include <type_traits>
#include <utility>

#include "core/assert_cast.h"
#include "core/custom_allocator.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/wkb_parse.h"

namespace doris {

// Apple ARM64 long double has binary64 precision/range. Keep native wide arithmetic
// where available; software quad preserves the tiny residual products on narrower ABIs.
using GeographyFloat =
        std::conditional_t<std::numeric_limits<long double>::digits >= 64 &&
                                   std::numeric_limits<long double>::min_exponent <= -4096,
                           long double, boost::multiprecision::cpp_bin_float_quad>;
static_assert(std::numeric_limits<GeographyFloat>::digits >= 64 &&
              std::numeric_limits<GeographyFloat>::min_exponent <= -4096);

template <typename Float>
static Float geography_pi() {
    if constexpr (std::is_same_v<Float, long double>) {
        return std::numbers::pi_v<long double>;
    } else {
        // Public software acos avoids float128 constant conversions/libquadmath on GCC.
        static const Float pi = boost::multiprecision::acos(Float(-1));
        return pi;
    }
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
template <typename Float = GeographyFloat>
static std::pair<Float, Float> geography_degree_sincos(double a, double b = 0, bool half = false) {
    const auto [high, low] = geography_degree_sum(a, b);
    int quadrant;
    const Float reduced = [&] {
        if constexpr (std::is_same_v<Float, long double>) {
            return std::remquo(static_cast<Float>(high), half ? 180.0L : 90.0L, &quadrant) +
                   static_cast<Float>(low);
        } else {
            // high is binary64 and the periods are exact integers: its reduced remainder
            // is representable in binary64. Add TwoSum's low residual only after widening.
            return Float(std::remquo(high, half ? 180.0 : 90.0, &quadrant)) + Float(low);
        }
    }();
    const Float radians = reduced * (geography_pi<Float>() / 180) / (half ? 2 : 1);
    using std::cos;
    using std::sin;
    const Float sine = sin(radians);
    const Float cosine = cos(radians);
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
template <typename Float = GeographyFloat>
struct GeographyRawArc {
    S2Point normal;
    S2Point midpoint;

    GeographyRawArc(const GeoCoordinate& a, const GeoCoordinate& b) {
        const auto [sd, cd] = geography_degree_sincos<Float>(b.x, -a.x, true);
        const auto [sm, cm] = geography_degree_sincos<Float>(a.x, b.x, true);
        const auto rotate_unit = [&](Float x, Float y, Float z) {
            using std::sqrt;
            const Float norm = sqrt(x * x + y * y + z * z);
            DCHECK_GT(norm, 0);
            return S2Point(static_cast<double>((cm * x - sm * y) / norm),
                           static_cast<double>((sm * x + cm * y) / norm),
                           static_cast<double>(z / norm));
        };
        normal = rotate_unit(-sd * geography_degree_sincos<Float>(a.y, b.y).first,
                             cd * geography_degree_sincos<Float>(a.y, -b.y).first,
                             geography_degree_sincos<Float>(a.y).second *
                                     geography_degree_sincos<Float>(b.y).second *
                                     geography_degree_sincos<Float>(b.x, -a.x).first);
        const auto [sl, cl] = geography_degree_sincos<Float>(a.y, b.y, true);
        const auto [st, ct] = geography_degree_sincos<Float>(b.y, -a.y, true);
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

static bool geography_stable_points(const DorisVector<GeographyVertex>& sequence,
                                    std::vector<S2Point>* points) {
    for (size_t i = 0; i + 1 < sequence.size(); ++i) {
        const auto& a = sequence[i];
        const auto& b = sequence[i + 1];
        // (0,0)->(180,0) has no unique arc and would normalize a zero raw normal.
        if (geography_antipodal(a, b)) {
            return false;
        }
        points->push_back(a.point);
        // Stabilize before S2 admission: rounded (30,58)->(-150,-58-e) can be
        // exactly antipodal despite its unique raw arc passing through the south pole.
        if (a.point.DotProd(b.point) < 0) {
            points->push_back(GeographyRawArc(a.coordinate, b.coordinate).midpoint);
        }
    }
    // A line's final vertex is real, including its closing edge; only rings remove it.
    points->push_back(sequence.back().point);
    points->erase(std::ranges::unique(*points).begin(), points->end());
    return true;
}

static std::unique_ptr<GeoShape> decode_geography_line(const GeographyCoordinates& raw,
                                                       StringRef value) {
    std::vector<S2Point> points;
    if (!geography_stable_points(raw.sequences.front(), &points)) {
        return nullptr;
    }
    if (points.size() < 2) {
        // Canonical pole aliases such as (0,90),(90,90) collapse to one calculation
        // point. Preserve their prior admission; the old parser still rejects a
        // truly duplicate-only line such as (1,1),(1,1).
        GeoParseStatus status;
        return GeoShape::from_wkb_bytes(value.data, value.size, status);
    }
    auto polyline = std::make_unique<S2Polyline>(points, S2Debug::DISABLE);
    if (!polyline->IsValid()) {
        return nullptr;
    }
    return GeoLine::create_unique(std::move(polyline));
}

std::unique_ptr<GeoShape> decode_geography_calculation_shape(StringRef value) {
    GeographyCoordinates raw(value);
    if (raw.status != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }
    if (raw.type == GeoShapeType::GEO_SHAPE_POINT) {
        GeoParseStatus status;
        return GeoShape::from_wkb_bytes(value.data, value.size, status);
    }
    if (raw.type == GeoShapeType::GEO_SHAPE_LINE_STRING) {
        return decode_geography_line(raw, value);
    }
    std::vector<std::unique_ptr<S2Loop>> loops;
    std::vector<const S2Loop*> input_loops;
    for (const auto& ring : raw.sequences) {
        std::vector<S2Point> points;
        // Validate holes against stable raw arcs, as with typed line admission.
        if (!geography_stable_points(ring, &points)) {
            return nullptr;
        }
        // Validated WKB rings are closed; only polygon admission removes that endpoint.
        DCHECK(points.front() == points.back());
        points.pop_back();
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
// Keep this exact cancellation proof together during the behavior-preserving module extraction.
// NOLINTNEXTLINE(readability-function-size)
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
    using LineEdge = std::pair<GeographyVertex, GeographyVertex>;
    using LineEdges = std::set<LineEdge, std::less<>, CustomStdAllocator<LineEdge>>;
    using PolygonEdges = std::array<LineEdges, 2>;

    struct RawEdges {
        LineEdges line;
        PolygonEdges polygon;

        auto vertices(const GeographyCoordinates& raw) const {
            std::set<GeographyVertex, std::less<>, CustomStdAllocator<GeographyVertex>> result;
            // A pole-alias line can have no edges but still denotes one canonical position.
            result.insert(raw.sequences.front().front());
            const auto append = [&](const LineEdges& edges) {
                for (const auto& [a, b] : edges) {
                    result.insert(a);
                    result.insert(b);
                }
            };
            append(line);
            for (const auto& ring_edges : polygon) {
                append(ring_edges);
            }
            return result;
        }
    };

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
        // Polygon rings retain directed edges, unlike the merged line intervals. Raw
        // coordinate bands avoid recalling a whole tiny zigzag ring for every edge.
        if (left.type == GeoShapeType::GEO_SHAPE_POLYGON ||
            right.type == GeoShapeType::GEO_SHAPE_POLYGON) {
            _longitude_order.assign(coordinates.begin(), coordinates.end());
            _latitude_order = _longitude_order;
            std::ranges::sort(_latitude_order, {},
                              [](const auto& vertex) { return vertex.coordinate.y; });
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

    bool line_contains_point(const GeographyCoordinates& shape,
                             const GeographyVertex& point) const {
        const auto& line = shape.sequences.front();
        for (size_t i = 0; i + 1 < line.size(); ++i) {
            if (contains_point(line[i], line[i + 1], point)) {
                return true;
            }
        }
        return false;
    }

    bool line_boundary_touches(const GeographyCoordinates& line,
                               const GeographyCoordinates& other) const {
        const auto& vertices = line.sequences.front();
        // Closed lines have no boundary. An open endpoint must belong to the other raw line:
        // a rounded coincidence across the gap 58 -> 58+ulp is not a contact.
        return vertices.front() != vertices.back() &&
               (line_contains_point(other, vertices.front()) ||
                line_contains_point(other, vertices.back()));
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
        DorisVector<GeographyVertex> on_edge;
        const auto append = [&](const GeographyVertex& point) {
            const auto& c = point.coordinate;
            // Longitude is strictly monotone along every non-meridian minor arc.
            // Retain the raw interval and exact proof for both candidate strategies.
            const bool within = wraps ? (c.x < std::min(a.x, b.x) || c.x > std::max(a.x, b.x))
                                      : (c.x > std::min(a.x, b.x) && c.x < std::max(a.x, b.x));
            if (within && geography_common_great_circle(a, b, c)) {
                on_edge.push_back(point);
            }
        };
        if (!visit_small_raw_candidates(start, end, wraps, append)) {
            // Near-antipodal conversion can miss raw nodes. Preserve the existing
            // stable half-arc recall whenever a small raw band is unavailable.
            const auto midpoint = GeographyRawArc(a, b).midpoint;
            for (const auto& endpoint : {start, end}) {
                visit_edge_candidates(
                        endpoint.point, midpoint, [](double) { return false; }, append);
            }
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

    void add_line(const GeographyCoordinates& shape, MutableS2ShapeIndex* index,
                  LineEdges* raw_line_edges = nullptr) const {
        using Interval = std::pair<double, double>;
        DorisMap<const DorisVector<Vertex>*, DorisVector<Interval>> intervals;
        LineEdges raw_edges;
        const auto& line = shape.sequences.front();
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
        LineEdges edges;
        auto add_edge = [&](const GeographyVertex& a, const GeographyVertex& b) {
            // Equality must distinguish (0,58)->(0,59) from (0,58+ulp)->(0,59),
            // even when conversion collapses the raw 58->58+ulp subedge to one S2Point.
            if (raw_line_edges != nullptr) {
                raw_line_edges->emplace(std::min(a, b), std::max(a, b));
            }
            // Rounded adjacent vertices can share one S2Point while remaining distinct raw
            // WKB vertices, for example 58 and 58.00000000000001 degrees. The raw edge is
            // retained for identity, but its zero-length S2 representation must not become
            // an OPEN/AA boundary marker at the original line endpoint.
            if (a.point == b.point) {
                return;
            }
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
        add_line_shapes(line, edges, index);
    }

private:
    template <typename Visit>
    bool visit_small_raw_candidates(const GeographyVertex& start, const GeographyVertex& end,
                                    bool wraps, Visit&& visit) const {
        // The S2 recall radius can cover an entire 1e-14-degree ring. Ordinary
        // edges keep the spatial query; this threshold only selects an index.
        constexpr double recall_radius = 64 * std::numeric_limits<double>::epsilon();
        if ((start.point - end.point).Norm2() > recall_radius * recall_radius) {
            return false;
        }
        const auto& a = start.coordinate;
        const auto& b = end.coordinate;
        const auto longitude = [](const auto& vertex) { return vertex.coordinate.x; };
        const auto lo =
                std::ranges::upper_bound(_longitude_order, std::min(a.x, b.x), {}, longitude);
        const auto hi =
                std::ranges::lower_bound(_longitude_order, std::max(a.x, b.x), {}, longitude);
        auto first = wraps ? _longitude_order.begin() : lo;
        auto last = hi;
        auto second = wraps ? lo : _longitude_order.end();
        if (wraps) {
            // At the date-line cut the strict longitude interval has two pieces.
            last = std::ranges::lower_bound(_longitude_order, std::min(a.x, b.x), {}, longitude);
            second = std::ranges::upper_bound(_longitude_order, std::max(a.x, b.x), {}, longitude);
        }
        size_t count = (last - first) + (_longitude_order.end() - second);
        const double dx = std::nextafter(std::abs(b.x - a.x), INFINITY);
        const double dy = std::nextafter(std::abs(b.y - a.y), 0.0);
        // For |A|,|B|<=45 and |dLon|<=90, endpoint latitude tangents have signs of
        // sin(B-A) plus sin(A)cos(B)*(1-cos(dLon)), or minus sin(B)cos(A)*(1-cos(dLon)).
        // In degrees, |sin(dLat)|
        // >= |dLat|/90 and 1-cos(dLon) < dLon^2/5000. Thus dy>dx^2 proves
        // monotonicity, with outward rounding below. A (0,45)->(90,45) arc
        // fails this certificate: its latitude bows above both endpoints.
        if (std::abs(a.y) <= 45 && std::abs(b.y) <= 45 && dx <= 90 &&
            dy > std::nextafter(dx * dx, INFINITY)) {
            const auto latitude = [](const auto& vertex) { return vertex.coordinate.y; };
            const auto from =
                    std::ranges::lower_bound(_latitude_order, std::min(a.y, b.y), {}, latitude);
            const auto to =
                    std::ranges::upper_bound(_latitude_order, std::max(a.y, b.y), {}, latitude);
            if (static_cast<size_t>(to - from) < count) {
                first = from;
                last = to;
                second = _longitude_order.end();
                count = to - from;
            }
        }
        // Eight is only a strategy crossover, never a result cap. Larger or
        // unproven bands use the complete existing S2 recall without truncation.
        if (count > 8) {
            return false;
        }
        for (const auto& range :
             {std::pair {first, last}, std::pair {second, _longitude_order.end()}}) {
            for (auto point = range.first; point != range.second; ++point) {
#ifdef BE_TEST
                ++geography_candidate_visits;
#endif
                visit(*point);
            }
        }
        return true;
    }

    void add_line_shapes(const DorisVector<GeographyVertex>& line, const LineEdges& edges,
                         MutableS2ShapeIndex* index) const {
        const auto& first = line.front();
        const auto& last = line.back();
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
                // The pinned S2 unary transform repeatedly moves a stateless lambda.
                // Component negation is identical and avoids that analyzer false positive.
                normal_index.Add(S2Point(-normal.x(), -normal.y(), -normal.z()), group);
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
        // Explicit component negation avoids the pinned S2 transform's repeated moves of
        // a stateless lambda (clang-analyzer-cplusplus.Move), preserving signed zero too.
        const std::array<S2Point, 4> quarter {
                u, v, {-u.x(), -u.y(), -u.z()}, {-v.x(), -v.y(), -v.z()}};
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
    DorisVector<GeographyVertex> _longitude_order;
    DorisVector<GeographyVertex> _latitude_order;
};

static void geography_shape_index(const GeoShape& shape, const GeographyCoordinates& raw,
                                  const GeographyEdgeNodes& nodes, MutableS2ShapeIndex* index,
                                  GeographyEdgeNodes::RawEdges* raw_edges = nullptr) {
    switch (shape.type()) {
    case GeoShapeType::GEO_SHAPE_POINT:
        index->Add(std::make_unique<S2PointVectorShape>(
                std::vector<S2Point> {raw.sequences.front().front().point}));
        break;
    case GeoShapeType::GEO_SHAPE_LINE_STRING:
        nodes.add_line(raw, index, raw_edges == nullptr ? nullptr : &raw_edges->line);
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
                    // Keep raw apex 58 distinct from 58+ulp before S2 conversion. Depth parity
                    // preserves shell/hole roles; duplicate aliases and synthetic points add no edge.
                    if (raw_edges != nullptr && edge[k] != edge[k + 1]) {
                        raw_edges->polygon[depths[i] % 2].emplace(std::min(edge[k], edge[k + 1]),
                                                                  std::max(edge[k], edge[k + 1]));
                    }
                    vertices.push_back(edge[k].point);
                    // Use the same stable long arcs for interiors and distances.
                    // The raw (-45+e,-45)->(135-e,45) edge otherwise changes sides
                    // around POINT(45 0.000001) when its midpoint is inserted.
                    if (const auto midpoint = nodes.arc_midpoint(edge[k], edge[k + 1])) {
                        vertices.push_back(*midpoint);
                    }
                }
            }
            // Noding (0,58)->(0,59) at raw 58+ulp can repeat its rounded endpoint,
            // including across the ring seam. Lax zero-length edges change OPEN boundary
            // ownership; retain the raw edges above but compact this calculation-only ring.
            vertices.erase(std::ranges::unique(vertices).begin(), vertices.end());
            if (vertices.front() == vertices.back()) {
                vertices.pop_back();
            }
            DCHECK_GE(vertices.size(), 3);
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

// Compare only nonshared raw edges with the already built rounded polygon boundary.
// Open segments omit isolated endpoint contacts; call this only after polygon interiors
// were found disjoint, so a real transverse crossing cannot masquerade as boundary overlap.
static bool geography_rounded_boundary_contact(const GeographyEdgeNodes::RawEdges& source,
                                               const GeographyEdgeNodes::RawEdges& polygon,
                                               const S2ShapeIndex& polygon_index,
                                               const GeographyEdgeNodes& nodes) {
    if (polygon.polygon[0].empty() && polygon.polygon[1].empty()) {
        return false;
    }
    MutableS2ShapeIndex boundary;
    MutableS2ShapeIndex distinct;
    for (const auto* shape : polygon_index) {
        for (int i = 0; i < shape->num_edges(); ++i) {
            const auto edge = shape->edge(i);
            boundary.Add(std::make_unique<GeographyLineShape>(edge.v0, edge.v1));
        }
    }
    for (const auto* edges : {&source.line, source.polygon.data(), source.polygon.data() + 1}) {
        for (const auto& edge : *edges) {
            if (polygon.polygon[0].contains(edge) || polygon.polygon[1].contains(edge)) {
                continue;
            }
            if (const auto midpoint = nodes.arc_midpoint(edge.first, edge.second)) {
                distinct.Add(std::make_unique<GeographyLineShape>(edge.first.point, *midpoint));
                distinct.Add(std::make_unique<GeographyLineShape>(*midpoint, edge.second.point));
            } else {
                distinct.Add(
                        std::make_unique<GeographyLineShape>(edge.first.point, edge.second.point));
            }
        }
    }
    S2BooleanOperation::Options options;
    options.set_polyline_model(S2BooleanOperation::PolylineModel::OPEN);
    options.set_polyline_loops_have_boundaries(true);
    return S2BooleanOperation::Intersects(distinct, boundary, options);
}

static bool geography_rounded_vertex_collision(const GeographyEdgeNodes::RawEdges& left_edges,
                                               const GeographyCoordinates& left_raw,
                                               const GeographyEdgeNodes::RawEdges& right_edges,
                                               const GeographyCoordinates& right_raw) {
    const auto a = left_edges.vertices(left_raw);
    const auto b = right_edges.vertices(right_raw);
    const auto collision = [](const auto& source, const auto& other) {
        std::set<S2Point, std::less<>, CustomStdAllocator<S2Point>> distinct;
        for (const auto& vertex : source) {
            if (!other.contains(vertex)) {
                distinct.insert(vertex.point);
            }
        }
        return std::ranges::any_of(
                other, [&](const auto& vertex) { return distinct.contains(vertex.point); });
    };
    return collision(a, b) || collision(b, a);
}

static Status geography_check_polygon_interiors(std::string_view name, const S2ShapeIndex& left,
                                                const S2ShapeIndex& right,
                                                const GeographyEdgeNodes& nodes,
                                                const GeographyEdgeNodes::RawEdges& left_edges,
                                                const GeographyEdgeNodes::RawEdges& right_edges,
                                                bool interior) {
    // Raw longitude 58+ulp can become meridian 58 in S2, losing a line or polygon interior.
    // Check batched boundary contacts, including partial arcs, instead of guessing the raw region.
    if (!interior && (geography_rounded_boundary_contact(left_edges, right_edges, right, nodes) ||
                      geography_rounded_boundary_contact(right_edges, left_edges, left, nodes))) {
        return Status::NotSupported(
                "{} does not support ambiguous spherical Polygon boundary interiors", name);
    }
    return Status::OK();
}

static S1ChordAngle geography_conversion_error();

// Return the side of a raw spherical edge using the original degree values. The
// calculation index uses binary64 S2 points, so a point one ULP outside an edge
// can move to the other side after conversion. Keep this check local to points
// already within the conversion error of a polygon edge; distant points cannot
// be affected by the bounded coordinate conversion.
static int geography_raw_side(const GeoCoordinate& a, const GeoCoordinate& b,
                              const GeoCoordinate& point) {
    using RawSideFloat = boost::multiprecision::cpp_bin_float_quad;
    const auto vector = [](const GeoCoordinate& coordinate) {
        const auto [sin_lon, cos_lon] =
                geography_degree_sincos<RawSideFloat>(coordinate.x);
        const auto [sin_lat, cos_lat] =
                geography_degree_sincos<RawSideFloat>(coordinate.y);
        return std::array<RawSideFloat, 3> {cos_lat * cos_lon, cos_lat * sin_lon, sin_lat};
    };
    const auto first = vector(a);
    const auto second = vector(b);
    const auto query = vector(point);
    const RawSideFloat determinant =
            (first[1] * second[2] - first[2] * second[1]) * query[0] +
            (first[2] * second[0] - first[0] * second[2]) * query[1] +
            (first[0] * second[1] - first[1] * second[0]) * query[2];
    return (determinant > 0) - (determinant < 0);
}

static bool geography_raw_rounded_side_mismatch(const GeographyCoordinates& polygon,
                                                const GeographyVertex& point) {
    const S1Angle conversion_error = geography_conversion_error().ToAngle();
    for (const auto& ring : polygon.sequences) {
        for (size_t i = 0; i + 1 < ring.size(); ++i) {
            const auto& start = ring[i];
            const auto& end = ring[i + 1];
            if (start == end || point == start || point == end ||
                S2::GetDistance(point.point, start.point, end.point) > conversion_error) {
                continue;
            }
            const int raw_side = geography_raw_side(start.coordinate, end.coordinate,
                                                     point.coordinate);
            const int rounded_side =
                    s2pred::ExpensiveSign(start.point, end.point, point.point, false);
            if (raw_side != 0 && rounded_side != 0 && raw_side != rounded_side) {
                return true;
            }
        }
    }
    return false;
}

static Status geography_point_relation(std::string_view name, const GeoShape& lhs,
                                       const GeoShape& rhs, const GeographyEdgeNodes& nodes,
                                       bool* result) {
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
        const bool member = nodes.line_contains_point(other_raw, point);
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
        if (!boundary && name != "st_touches") {
            MutableS2ShapeIndex index;
            MutableS2ShapeIndex point_index;
            geography_shape_index(other, other_raw, nodes, &index);
            geography_shape_index(lhs.type() == GeoShapeType::GEO_SHAPE_POINT ? lhs : rhs,
                                  point_raw, nodes, &point_index);
            S2BooleanOperation::Options options;
            options.set_polygon_model(S2BooleanOperation::PolygonModel::CLOSED);
            const bool closed = S2BooleanOperation::Contains(index, point_index, options);
            options.set_polygon_model(S2BooleanOperation::PolygonModel::OPEN);
            interior = S2BooleanOperation::Contains(index, point_index, options);
            // Raw POINT(58+ulp 0) is not on the boundary at vertex (58,0). Do not let
            // SEMI_OPEN vertex ownership turn this rounded boundary coincidence into an interior.
            if (closed && !interior) {
                return Status::NotSupported(
                        "{} does not support ambiguous spherical Polygon boundary ownership", name);
            }
            if (geography_raw_rounded_side_mismatch(other_raw, point)) {
                return Status::NotSupported(
                        "{} does not support ambiguous spherical Polygon boundary side conversion",
                        name);
            }
        }
    }
    if (name == "st_intersects") {
        *result = interior || boundary;
    } else if (name == "st_disjoint") {
        *result = !interior && !boundary;
    } else if (name == "st_touches") {
        *result = boundary;
    } else {
        *result = interior;
    }
    return Status::OK();
}

// Preserve the existing operation-specific raw/S2 admission order in this mechanical move;
// decomposing these precision branches would mix algorithm changes into the extraction.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
Status geography_relation(std::string_view name, const GeoShape& lhs, const GeoShape& rhs,
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
        return geography_point_relation(name, lhs, rhs, nodes, result);
    }
    MutableS2ShapeIndex left;
    MutableS2ShapeIndex right;
    const GeographyEdgeNodes nodes(left_wkb, right_wkb);
    GeographyEdgeNodes::RawEdges left_edges;
    GeographyEdgeNodes::RawEdges right_edges;
    const bool raw_polygon_coverage = lhs.type() == GeoShapeType::GEO_SHAPE_POLYGON &&
                                      rhs.type() == GeoShapeType::GEO_SHAPE_POLYGON &&
                                      (containment || name == "st_equals" || name == "st_overlaps");
    // (0,58)->(0,59) covers a longer raw interval than (0,58+ulp)->(0,59),
    // even when their S2 endpoints collide. Reuse the shared noding for these relations.
    const bool raw_line_coverage =
            container.type() == GeoShapeType::GEO_SHAPE_LINE_STRING &&
            value.type() == GeoShapeType::GEO_SHAPE_LINE_STRING &&
            (containment || name == "st_equals" || name == "st_overlaps" || name == "st_touches");
    const bool raw_coverage = raw_line_coverage || raw_polygon_coverage || containment ||
                              name == "st_touches" || name == "st_overlaps";
    geography_shape_index(container, name == "st_within" ? nodes.right : nodes.left, nodes, &left,
                          raw_coverage ? &left_edges : nullptr);
    geography_shape_index(value, name == "st_within" ? nodes.left : nodes.right, nodes, &right,
                          raw_coverage ? &right_edges : nullptr);
    S2BooleanOperation::Options options;
    options.set_polygon_model(S2BooleanOperation::PolygonModel::CLOSED);
    options.set_polyline_model(S2BooleanOperation::PolylineModel::CLOSED);
    options.set_polyline_loops_have_boundaries(false);
    const auto shares_raw_line_edge = [&] {
        return std::ranges::any_of(
                left_edges.line, [&](const auto& edge) { return right_edges.line.contains(edge); });
    };
    if (name == "st_equals") {
        *result = left_edges.line == right_edges.line &&
                  left_edges.polygon == right_edges.polygon &&
                  S2BooleanOperation::Equals(left, right, options);
        return Status::OK();
    }
    // Different raw triangle apexes can produce the same S2 region. Boundary identity decides
    // equality, but cannot decide which raw region contains the other; do not accept a false bool.
    if (raw_polygon_coverage && left_edges.polygon != right_edges.polygon &&
        S2BooleanOperation::Equals(left, right, options)) {
        return Status::NotSupported(
                "{} does not support distinct raw Polygon boundaries with the same spherical "
                "representation",
                name);
    }
    // An exterior raw apex or line endpoint at 58+ulp can round onto 58 while
    // the remaining vertices lie inside. Check before coverage accepts either shape.
    const bool polygon_line_coverage = containment &&
                                       container.type() == GeoShapeType::GEO_SHAPE_POLYGON &&
                                       value.type() == GeoShapeType::GEO_SHAPE_LINE_STRING;
    if ((raw_polygon_coverage || polygon_line_coverage) &&
        geography_rounded_vertex_collision(
                left_edges, name == "st_within" ? nodes.right : nodes.left, right_edges,
                name == "st_within" ? nodes.left : nodes.right)) {
        return Status::NotSupported(
                "{} does not support ambiguous spherical Polygon boundary interiors", name);
    }
    if (containment && container.type() == GeoShapeType::GEO_SHAPE_LINE_STRING &&
        value.type() == GeoShapeType::GEO_SHAPE_POLYGON) {
        *result = false;
        return Status::OK();
    }
    const auto covers = [&](bool reverse) {
        const auto& a = reverse ? right_edges.line : left_edges.line;
        const auto& b = reverse ? left_edges.line : right_edges.line;
        return (!raw_line_coverage || std::ranges::includes(a, b)) &&
               S2BooleanOperation::Contains(reverse ? right : left, reverse ? left : right,
                                            options);
    };
    // Rounded indexes alone would let the shorter raw line contain the longer one.
    if (containment && !covers(false)) {
        *result = false;
        return Status::OK();
    }
    auto interior_options = options;
    interior_options.set_polygon_model(S2BooleanOperation::PolygonModel::OPEN);
    interior_options.set_polyline_model(S2BooleanOperation::PolylineModel::OPEN);
    if (containment) {
        // CLOSED Contains is covers: a polygon must not contain a line solely on its boundary.
        *result = S2BooleanOperation::Intersects(left, right, interior_options);
        return geography_check_polygon_interiors(name, left, right, nodes, left_edges, right_edges,
                                                 *result);
    }
    if (name == "st_overlaps") {
        // Neither equality nor containment is a partial overlap.
        if (covers(false) || covers(true)) {
            *result = false;
            return Status::OK();
        }
        if (lhs.type() == GeoShapeType::GEO_SHAPE_POLYGON) {
            *result = S2BooleanOperation::Intersects(left, right, interior_options);
            return geography_check_polygon_interiors(name, left, right, nodes, left_edges,
                                                     right_edges, *result);
        }
        // A crossing point has no shared raw edge. Conversely, a positive raw overlap
        // such as (0,58)->(0,58+ulp) must survive even when S2 rounds it to one point.
        *result = shares_raw_line_edge();
        return Status::OK();
    }
    bool intersects = S2BooleanOperation::Intersects(left, right, options);
    // A nearby disjoint curve must not become a boundary contact through the legacy
    // conversion-distance fallback used below by Intersects/Disjoint.
    if (name == "st_touches") {
        *result = intersects && !S2BooleanOperation::Intersects(left, right, interior_options);
        // CLOSED/OPEN alone treats a raw gap or a positive overlap 58->58+ulp as one
        // endpoint contact. A shared raw edge has intersecting interiors, so cannot touch.
        if (*result && lhs.type() == GeoShapeType::GEO_SHAPE_LINE_STRING &&
            rhs.type() == GeoShapeType::GEO_SHAPE_LINE_STRING) {
            const auto left_vertices = left_edges.vertices(nodes.left);
            const auto right_vertices = right_edges.vertices(nodes.right);
            const auto interior_node = [](const GeographyCoordinates& raw,
                                          const GeographyVertex& vertex) {
                const auto& line = raw.sequences.front();
                return line.front() == line.back() ||
                       (vertex != line.front() && vertex != line.back());
            };
            // In (0,58)->(0,59), raw node (0,58+ulp) is interior even when S2 maps it
            // onto the first endpoint. A separate genuine endpoint contact at (0,59)
            // must not hide that raw interior crossing. Reuse the existing noded vertices.
            const bool shared_interior =
                    std::ranges::any_of(left_vertices, [&](const auto& vertex) {
                        return right_vertices.contains(vertex) &&
                               interior_node(nodes.left, vertex) &&
                               interior_node(nodes.right, vertex);
                    });
            *result = !shared_interior && !shares_raw_line_edge() &&
                      (nodes.line_boundary_touches(nodes.left, nodes.right) ||
                       nodes.line_boundary_touches(nodes.right, nodes.left));
        } else if (*result) {
            // A line endpoint at latitude 58 and polygon apex at 58+ulp can share an S2Point
            // without raw contact. Existing noding supplies true vertex/edge contacts, including
            // holes and overlapping boundaries, without treating synthetic midpoints as witnesses.
            const auto left_vertices = left_edges.vertices(nodes.left);
            const auto right_vertices = right_edges.vertices(nodes.right);
            *result = std::ranges::any_of(left_vertices, [&](const auto& vertex) {
                return right_vertices.contains(vertex);
            });
            // No raw boundary contact already proves Touches=false; only confirmed contacts
            // require the rounded-interior precision check (e.g. an inward 58+ulp line).
            if (*result) {
                RETURN_IF_ERROR(geography_check_polygon_interiors(name, left, right, nodes,
                                                                  left_edges, right_edges, false));
            }
            const auto& line =
                    lhs.type() == GeoShapeType::GEO_SHAPE_LINE_STRING ? left_edges : right_edges;
            const auto& polygon =
                    lhs.type() == GeoShapeType::GEO_SHAPE_POLYGON ? left_edges : right_edges;
            // Raw (0,58)->(0,58+ulp) can enter a polygon although its S2 edge disappears.
            // Only a shared raw boundary edge proves this collapsed segment stays on the boundary.
            if (*result && std::ranges::any_of(line.line, [&](const auto& edge) {
                    return edge.first.point == edge.second.point &&
                           !polygon.polygon[0].contains(edge) && !polygon.polygon[1].contains(edge);
                })) {
                return Status::NotSupported(
                        "st_touches does not support collapsed raw line edges outside the Polygon "
                        "boundary");
            }
        }
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

template <typename Float>
static bool geography_bearing_impl(StringRef from, StringRef to, double* bearing) {
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
    const auto [sin_half, cos_half] = geography_degree_sincos<Float>(end.first, -start.first, true);
    const Float east = geography_degree_sincos<Float>(end.second).second *
                       geography_degree_sincos<Float>(end.first, -start.first).first;
    const Float north =
            geography_degree_sincos<Float>(end.second, -start.second).first * cos_half * cos_half +
            geography_degree_sincos<Float>(end.second, start.second).first * sin_half * sin_half;
    using std::atan2;
    *bearing = static_cast<double>(atan2(east, north));
    *bearing = std::fmod(*bearing + 2 * M_PI, 2 * M_PI);
    return true;
}

bool geography_bearing(StringRef from, StringRef to, double* bearing) {
    return geography_bearing_impl<GeographyFloat>(from, to, bearing);
}

#ifdef BE_TEST
std::array<double, 6> geography_portable_arc_for_test(double ax, double ay, double bx, double by) {
    const GeographyRawArc<boost::multiprecision::cpp_bin_float_quad> arc({ax, ay}, {bx, by});
    return {arc.normal.x(),   arc.normal.y(),   arc.normal.z(),
            arc.midpoint.x(), arc.midpoint.y(), arc.midpoint.z()};
}

bool geography_portable_bearing_for_test(StringRef from, StringRef to, double* bearing) {
    return geography_bearing_impl<boost::multiprecision::cpp_bin_float_quad>(from, to, bearing);
}
#endif

double geography_distance(const GeoShape& left_shape, const GeoShape& right_shape,
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

double geography_area(const GeoShape& shape, StringRef wkb, bool square_meters) {
    GeographyEdgeNodes nodes(wkb, wkb);
    MutableS2ShapeIndex index;
    geography_shape_index(shape, nodes.left, nodes, &index);
    const double steradians = S2::GetArea(*index.shape(0));
    return square_meters ? S2Earth::SteradiansToSquareMeters(steradians)
                         : S2Earth::SteradiansToSquareKm(steradians);
}
} // namespace doris
