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

#include "exprs/function/geo/wkb_parse.h"

#include <fmt/format.h>

#include <cmath>
#include <cstddef>
#include <istream>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/custom_allocator.h"
#include "exprs/function/geo/ByteOrderDataInStream.h"
#include "exprs/function/geo/ByteOrderValues.h"
#include "exprs/function/geo/geo_tobinary_type.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/wkb_parse_ctx.h"

namespace doris {

namespace {

constexpr const char* wkb_names[] = {
        "",           "POINT",           "LINESTRING",   "POLYGON",
        "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"};

GeoParseStatus read_wkb_type(WkbParseContext& ctx, uint32_t* type) {
    if (ctx.dis.size() < 5) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    const auto order = ctx.dis.readByte();
    if (order > 1) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    ctx.dis.setOrder(order == static_cast<unsigned char>(byteOrder::wkbNDR)
                             ? ByteOrderValues::EndianType::ENDIAN_LITTLE
                             : ByteOrderValues::EndianType::ENDIAN_BIG);
    auto encoded = ctx.dis.readUnsigned();
    constexpr uint32_t ewkb_metadata_flags = 0x80000000 | 0x40000000 | WKB_SRID_FLAG;
    // Typed XY rejects dimension/SRID metadata; e.g. EWKB SRID 4326 is legacy-only.
    // Historical STRING constructors retain their EWKB XY interpretation.
    if (!ctx.allow_ewkb_srid &&
        ((encoded & ewkb_metadata_flags) != 0 || (encoded >= 1000 && encoded < 4000))) {
        return GeoParseStatus::GEO_PARSE_WKB_UNSUPPORTED_METADATA;
    }
    if ((encoded & WKB_SRID_FLAG) != 0) {
        if (!ctx.allow_ewkb_srid || ctx.dis.size() < sizeof(uint32_t)) {
            return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
        }
        ctx.srid = static_cast<int>(ctx.dis.readUnsigned());
        encoded &= ~WKB_SRID_FLAG;
    }
    if (ctx.allow_ewkb_srid) {
        // Preserve the legacy parser: ignore EWKB dimension flags and read XY coordinates.
        // Typed constructors keep rejecting these flags above.
        encoded &= 0xFF;
    }
    // Typed values carry CRS in their schema and support only the XY base types.
    if (encoded < static_cast<uint32_t>(wkbType::wkbPoint) ||
        encoded > static_cast<uint32_t>(wkbType::wkbGeometryCollection)) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    *type = encoded;
    return GeoParseStatus::GEO_PARSE_OK;
}

// One cursor walks both validation and WKT output. Collection frames are bounded
// by the input size, rather than by the C++ call stack. Coordinates remain raw.
class RawWkbReader {
public:
    RawWkbReader(WkbParseContext& context, bool geographic, std::ostream* output = nullptr)
            : _ctx(context), _geographic(geographic), _output(output) {}

    GeoParseStatus read() {
        do {
            uint32_t type;
            const auto status = read_wkb_type(_ctx, &type);
            if (status != GeoParseStatus::GEO_PARSE_OK) {
                return status;
            }
            if (!matches_parent(type)) {
                return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
            }
            if (_frames.empty()) {
                _root_type = type;
            }
            if (_frames.empty() ||
                _frames.back().type == static_cast<uint32_t>(wkbType::wkbGeometryCollection)) {
                emit(wkb_names[type]);
                emit(" ");
            }
            if (type <= static_cast<uint32_t>(wkbType::wkbPolygon)) {
                if (!read_primitive(type)) {
                    return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
                }
            } else {
                uint32_t count;
                if (!read_count(&count) || count > _ctx.dis.size() / 5) {
                    return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
                }
                if (count != 0) {
                    emit("(");
                    _frames.push_back({type, count});
                    continue;
                }
                emit("EMPTY");
            }
            finish_member();
        } while (!_frames.empty());
        return _ctx.dis.size() == 0 ? GeoParseStatus::GEO_PARSE_OK
                                    : GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }

    uint32_t root_type() const { return _root_type; }

private:
    struct Frame {
        uint32_t type;
        uint32_t remaining;
    };

    bool matches_parent(uint32_t type) const {
        if (_frames.empty() ||
            _frames.back().type == static_cast<uint32_t>(wkbType::wkbGeometryCollection)) {
            return true;
        }
        return type == _frames.back().type - 3;
    }

    void finish_member() {
        while (!_frames.empty()) {
            if (--_frames.back().remaining != 0) {
                emit(", ");
                return;
            }
            emit(")");
            _frames.pop_back();
        }
    }

    void emit(std::string_view text) {
        if (_output != nullptr) {
            *_output << text;
        }
    }

    bool read_count(uint32_t* count) {
        if (_ctx.dis.size() < sizeof(uint32_t)) {
            return false;
        }
        *count = _ctx.dis.readUnsigned();
        return true;
    }

    bool read_xy(double* x, double* y) {
        if (_ctx.dis.size() < 2 * sizeof(double)) {
            return false;
        }
        *x = _ctx.dis.readDouble();
        *y = _ctx.dis.readDouble();
        return true;
    }

    bool valid_xy(double x, double y) const {
        return !std::isinf(x) && !std::isinf(y) &&
               (!_geographic || ((std::isnan(x) || (x >= -180 && x <= 180)) &&
                                 (std::isnan(y) || (y >= -90 && y <= 90))));
    }

    void emit_xy(double x, double y) {
        if (_output != nullptr) {
            *_output << (std::isnan(x) ? "NaN" : fmt::format("{}", x)) << " "
                     << (std::isnan(y) ? "NaN" : fmt::format("{}", y));
        }
    }

    bool read_point() {
        double x;
        double y;
        if (!read_xy(&x, &y)) {
            return false;
        }
        // NaN/NaN is the interoperable EMPTY Point convention, distinct from NULL.
        if (std::isnan(x) && std::isnan(y)) {
            emit("EMPTY");
            return true;
        }
        if (!valid_xy(x, y)) {
            return false;
        }
        emit("(");
        emit_xy(x, y);
        emit(")");
        return true;
    }

    bool read_coordinates(uint32_t count, bool ring) {
        if (count > _ctx.dis.size() / (2 * sizeof(double))) {
            return false;
        }
        double first_x = 0;
        double first_y = 0;
        double x = 0;
        double y = 0;
        for (uint32_t i = 0; i < count; ++i) {
            if (!read_xy(&x, &y) || !valid_xy(x, y)) {
                return false;
            }
            if (i == 0) {
                first_x = x;
                first_y = y;
            } else {
                emit(", ");
            }
            emit_xy(x, y);
        }
        return !ring || ((first_x == x || (std::isnan(first_x) && std::isnan(x))) &&
                         (first_y == y || (std::isnan(first_y) && std::isnan(y))));
    }

    bool read_line() {
        uint32_t count;
        if (!read_count(&count) || count == 1) {
            return false;
        }
        if (count == 0) {
            emit("EMPTY");
            return true;
        }
        emit("(");
        if (!read_coordinates(count, false)) {
            return false;
        }
        emit(")");
        return true;
    }

    bool read_polygon() {
        uint32_t rings;
        if (!read_count(&rings) || rings > _ctx.dis.size() / sizeof(uint32_t)) {
            return false;
        }
        if (rings == 0) {
            emit("EMPTY");
            return true;
        }
        emit("(");
        for (uint32_t i = 0; i < rings; ++i) {
            uint32_t count;
            if (!read_count(&count) || count < 4) {
                return false;
            }
            if (i != 0) {
                emit(", ");
            }
            emit("(");
            if (!read_coordinates(count, true)) {
                return false;
            }
            emit(")");
        }
        emit(")");
        return true;
    }

    bool read_primitive(uint32_t type) {
        switch (type) {
        case static_cast<uint32_t>(wkbType::wkbPoint):
            return read_point();
        case static_cast<uint32_t>(wkbType::wkbLine):
            return read_line();
        case static_cast<uint32_t>(wkbType::wkbPolygon):
            return read_polygon();
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "Unexpected WKB primitive type: {}", type);
        }
    }

    WkbParseContext& _ctx;
    const bool _geographic;
    std::ostream* const _output;
    uint32_t _root_type = 0;
    DorisVector<Frame> _frames;
};

} // namespace

unsigned char ASCIIHexToUChar(char val) {
    switch (val) {
    case '0':
        return 0;
    case '1':
        return 1;
    case '2':
        return 2;
    case '3':
        return 3;
    case '4':
        return 4;
    case '5':
        return 5;
    case '6':
        return 6;
    case '7':
        return 7;
    case '8':
        return 8;
    case '9':
        return 9;
    case 'A':
    case 'a':
        return 10;
    case 'B':
    case 'b':
        return 11;
    case 'C':
    case 'c':
        return 12;
    case 'D':
    case 'd':
        return 13;
    case 'E':
    case 'e':
        return 14;
    case 'F':
    case 'f':
        return 15;
    default:
        return static_cast<unsigned char>(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR);
    }
}

GeoParseStatus WkbParse::parse_wkb(std::istream& is, std::unique_ptr<GeoShape>& shape) {
    WkbParseContext ctx;
    // Legacy ST_GeometryFromWKB accepts EWKB with an embedded SRID. The SRID is
    // intentionally ignored because the legacy encoded representation has no CRS field.
    ctx.allow_ewkb_srid = true;

    WkbParse::read_hex(is, ctx);
    if (ctx.parse_status == GeoParseStatus::GEO_PARSE_OK) {
        shape = std::move(ctx.shape);
    }
    return ctx.parse_status;
}

GeoParseStatus WkbParse::initialize_context(const char* data, size_t size, WkbParseContext* ctx) {
    if (size == 0) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    const auto byte_order = static_cast<unsigned char>(data[0]);
    if (byte_order == static_cast<unsigned char>(byteOrder::wkbNDR)) {
        ctx->dis = ByteOrderDataInStream(reinterpret_cast<const unsigned char*>(data), size);
        ctx->dis.setOrder(ByteOrderValues::EndianType::ENDIAN_LITTLE);
    } else if (byte_order == static_cast<unsigned char>(byteOrder::wkbXDR)) {
        ctx->dis = ByteOrderDataInStream(reinterpret_cast<const unsigned char*>(data), size);
        ctx->dis.setOrder(ByteOrderValues::EndianType::ENDIAN_BIG);
    } else {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return GeoParseStatus::GEO_PARSE_OK;
}

GeoParseStatus WkbParse::parse_wkb_bytes(const char* data, size_t size,
                                         std::unique_ptr<GeoShape>& shape) {
    WkbParseContext ctx;
    const auto status = initialize_context(data, size, &ctx);
    // Empty input or an invalid byte-order header cannot initialize the byte reader.
    if (status != GeoParseStatus::GEO_PARSE_OK) {
        return status;
    }
    auto parsed = readGeometry(ctx);
    // Invalid line/ring coordinate lists and unsupported kinds cannot produce an S2 shape.
    if (!parsed) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    shape = std::move(parsed);
    return GeoParseStatus::GEO_PARSE_OK;
}

GeoParseStatus WkbParse::read_geometry_type(WkbParseContext& ctx, uint32_t* type) {
    return read_wkb_type(ctx, type);
}

GeoParseStatus WkbParse::wkb_to_wkt(const char* data, size_t size, std::string* wkt) {
    WkbParseContext ctx;
    if (initialize_context(data, size, &ctx) != GeoParseStatus::GEO_PARSE_OK) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    std::ostringstream os;
    const auto status = RawWkbReader(ctx, false, &os).read();
    if (status == GeoParseStatus::GEO_PARSE_OK) {
        *wkt = os.str();
    }
    return status;
}

GeoParseStatus WkbParse::point_coordinates(const char* data, size_t size, double* x, double* y) {
    WkbParseContext ctx;
    if (initialize_context(data, size, &ctx) != GeoParseStatus::GEO_PARSE_OK) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    uint32_t type;
    const auto status = read_geometry_type(ctx, &type);
    if (status != GeoParseStatus::GEO_PARSE_OK) {
        return status;
    }
    if (type != static_cast<uint32_t>(wkbType::wkbPoint) || ctx.dis.size() != 2 * sizeof(double)) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    *x = ctx.dis.readDouble();
    *y = ctx.dis.readDouble();
    return std::isinf(*x) || std::isinf(*y) || (std::isnan(*x) && std::isnan(*y))
                   ? GeoParseStatus::GEO_PARSE_COORD_INVALID
                   : GeoParseStatus::GEO_PARSE_OK;
}

GeoParseStatus WkbParse::geometry_type(const char* data, size_t size, std::string* type) {
    WkbParseContext ctx;
    if (initialize_context(data, size, &ctx) != GeoParseStatus::GEO_PARSE_OK) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    RawWkbReader reader(ctx, false);
    const auto status = reader.read();
    if (status == GeoParseStatus::GEO_PARSE_OK) {
        *type = std::string("ST_") + wkb_names[reader.root_type()];
    }
    return status;
}

GeoParseStatus WkbParse::validate_wkb_bytes(const char* data, size_t size, bool geographic) {
    WkbParseContext ctx;
    if (initialize_context(data, size, &ctx) != GeoParseStatus::GEO_PARSE_OK) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return RawWkbReader(ctx, geographic).read();
}

Status WkbParse::validate_spatial_wkb(const char* data, size_t size, bool geographic, size_t row) {
    const auto status = validate_wkb_bytes(data, size, geographic);
    if (status == GeoParseStatus::GEO_PARSE_WKB_UNSUPPORTED_METADATA) {
        return Status::NotSupported(
                "WKB dimensions or embedded SRID are not supported for spatial inputs at row {}",
                row);
    }
    if (status != GeoParseStatus::GEO_PARSE_OK) {
        return Status::InvalidArgument("Invalid WKB in spatial input at row {}: {}", row,
                                       to_string(status));
    }
    return Status::OK();
}

void WkbParse::read_hex(std::istream& is, WkbParseContext& ctx) {
    // setup input/output stream
    std::stringstream os(std::ios_base::binary | std::ios_base::in | std::ios_base::out);

    while (true) {
        const int input_high = is.get();
        if (input_high == std::char_traits<char>::eof()) {
            break;
        }

        const int input_low = is.get();
        if (input_low == std::char_traits<char>::eof()) {
            ctx.parse_status = GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
            return;
        }

        const char high = static_cast<char>(input_high);
        const char low = static_cast<char>(input_low);

        const unsigned char result_high = ASCIIHexToUChar(high);
        const unsigned char result_low = ASCIIHexToUChar(low);

        const auto value = static_cast<unsigned char>((result_high << 4) + result_low);

        // write the value to the output stream
        os << value;
    }
    WkbParse::read(os, ctx);
}

void WkbParse::read(std::istream& is, WkbParseContext& ctx) {
    is.seekg(0, std::ios::end);
    auto size = is.tellg();
    is.seekg(0, std::ios::beg);

    // Check if size is valid
    if (size <= 0) {
        ctx.parse_status = GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    std::vector<unsigned char> buf(static_cast<size_t>(size));
    if (!is.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(size))) {
        ctx.parse_status = GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    // Ensure we have at least one byte for byte order
    if (buf.empty()) {
        ctx.parse_status = GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    // First read the byte order using machine endian
    auto byteOrder = buf[0];

    // Create ByteOrderDataInStream with the correct byte order
    if (byteOrder == static_cast<unsigned char>(byteOrder::wkbNDR)) {
        ctx.dis = ByteOrderDataInStream(buf.data(), buf.size());
        ctx.dis.setOrder(ByteOrderValues::EndianType::ENDIAN_LITTLE);
    } else if (byteOrder == static_cast<unsigned char>(byteOrder::wkbXDR)) {
        ctx.dis = ByteOrderDataInStream(buf.data(), buf.size());
        ctx.dis.setOrder(ByteOrderValues::EndianType::ENDIAN_BIG);
    } else {
        ctx.parse_status = GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    std::unique_ptr<GeoShape> shape = readGeometry(ctx);
    if (!shape) {
        ctx.parse_status = GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    ctx.shape = std::move(shape);
}

std::unique_ptr<GeoShape> WkbParse::readGeometry(WkbParseContext& ctx) {
    try {
        // Ensure we have enough data to read
        if (ctx.dis.size() < 5) { // At least 1 byte for order and 4 bytes for type
            return nullptr;
        }

        uint32_t geometryType;
        if (read_geometry_type(ctx, &geometryType) != GeoParseStatus::GEO_PARSE_OK) {
            return nullptr;
        }

        std::unique_ptr<GeoShape> shape;

        switch (static_cast<wkbType>(geometryType)) {
        case wkbType::wkbPoint:
            shape = readPoint(ctx);
            break;
        case wkbType::wkbLine:
            shape = readLine(ctx);
            break;
        case wkbType::wkbPolygon:
            shape = readPolygon(ctx);
            break;
        default:
            return nullptr;
        }

        return shape;
    } catch (...) {
        // Handle any exceptions from reading operations
        return nullptr;
    }
}

std::unique_ptr<GeoPoint> WkbParse::readPoint(WkbParseContext& ctx) {
    GeoCoordinateList coords = WkbParse::readCoordinateList(1, ctx);
    if (coords.list.empty()) {
        return nullptr;
    }

    std::unique_ptr<GeoPoint> point = GeoPoint::create_unique();
    if (!point || point->from_coord(coords.list[0]) != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }

    return point;
}

std::unique_ptr<GeoLine> WkbParse::readLine(WkbParseContext& ctx) {
    uint32_t size = ctx.dis.readUnsigned();
    if (minMemSize(wkbType::wkbLine, size, ctx) != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }

    GeoCoordinateList coords = WkbParse::readCoordinateList(size, ctx);
    if (coords.list.empty()) {
        return nullptr;
    }

    std::unique_ptr<GeoLine> line = GeoLine::create_unique();
    if (!line || line->from_coords(coords) != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }

    return line;
}

std::unique_ptr<GeoPolygon> WkbParse::readPolygon(WkbParseContext& ctx) {
    uint32_t num_loops = ctx.dis.readUnsigned();
    if (minMemSize(wkbType::wkbPolygon, num_loops, ctx) != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }

    GeoCoordinateListList coordss;
    for (uint32_t i = 0; i < num_loops; ++i) {
        uint32_t size = ctx.dis.readUnsigned();
        if (size < 3) { // A polygon loop must have at least 3 points
            return nullptr;
        }

        auto coords = std::make_unique<GeoCoordinateList>();
        *coords = WkbParse::readCoordinateList(size, ctx);
        if (coords->list.empty()) {
            return nullptr;
        }
        coordss.add(std::move(coords));
    }

    std::unique_ptr<GeoPolygon> polygon = GeoPolygon::create_unique();
    if (!polygon || polygon->from_coords(coordss) != GeoParseStatus::GEO_PARSE_OK) {
        return nullptr;
    }

    return polygon;
}

GeoCoordinateList WkbParse::readCoordinateList(unsigned size, WkbParseContext& ctx) {
    GeoCoordinateList coords;
    for (uint32_t i = 0; i < size; i++) {
        if (!readCoordinate(ctx)) {
            return GeoCoordinateList();
        }
        unsigned int j = 0;
        GeoCoordinate coord;
        coord.x = ctx.ordValues[j++];
        coord.y = ctx.ordValues[j++];
        coords.add(coord);
    }
    return coords;
}

GeoParseStatus WkbParse::minMemSize(wkbType type, uint64_t size, WkbParseContext& ctx) {
    uint64_t minSize = 0;
    constexpr uint64_t minCoordSize = 2 * sizeof(double);
    //constexpr uint64_t minPtSize = (1+4) + minCoordSize;
    //constexpr uint64_t minLineSize = (1+4+4); // empty line
    constexpr uint64_t minLoopSize = 4; // empty loop
    //constexpr uint64_t minPolySize = (1+4+4); // empty polygon
    //constexpr uint64_t minGeomSize = minLineSize;

    switch (type) {
    case wkbType::wkbLine:
        minSize = size * minCoordSize;
        break;
    case wkbType::wkbPolygon:
        minSize = size * minLoopSize;
        break;
    default:
        // Only line coordinates and polygon loops use this size check.
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    if (ctx.dis.size() < minSize) {
        return GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return GeoParseStatus::GEO_PARSE_OK;
}
bool WkbParse::readCoordinate(WkbParseContext& ctx) {
    for (std::size_t i = 0; i < ctx.inputDimension; ++i) {
        ctx.ordValues[i] = ctx.dis.readDouble();
    }

    return true;
}

} // namespace doris
