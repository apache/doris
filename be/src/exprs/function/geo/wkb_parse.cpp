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

#include <cstddef>
#include <istream>
#include <sstream>
#include <utility>
#include <vector>

#include "exprs/function/geo/ByteOrderDataInStream.h"
#include "exprs/function/geo/ByteOrderValues.h"
#include "exprs/function/geo/geo_tobinary_type.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/wkb_parse_ctx.h"

namespace doris {

namespace {

bool ascii_hex_to_uchar(char value, unsigned char* result) {
    if (value >= '0' && value <= '9') {
        *result = static_cast<unsigned char>(value - '0');
        return true;
    }
    if (value >= 'A' && value <= 'F') {
        *result = static_cast<unsigned char>(value - 'A' + 10);
        return true;
    }
    if (value >= 'a' && value <= 'f') {
        *result = static_cast<unsigned char>(value - 'a' + 10);
        return true;
    }
    return false;
}

} // namespace

GeoParseStatus WkbParse::parse_wkb(std::istream& is, std::unique_ptr<GeoShape>& shape) {
    WkbParseContext ctx;

    WkbParse::read_hex(is, ctx);
    if (ctx.parse_status == GEO_PARSE_OK) {
        shape = std::move(ctx.shape);
    }
    return ctx.parse_status;
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
            ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
            return;
        }

        const char high = static_cast<char>(input_high);
        const char low = static_cast<char>(input_low);

        unsigned char result_high = 0;
        unsigned char result_low = 0;
        if (!ascii_hex_to_uchar(high, &result_high) || !ascii_hex_to_uchar(low, &result_low)) {
            ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
            return;
        }

        const auto value = static_cast<unsigned char>((result_high << 4) + result_low);
        os.put(static_cast<char>(value));
    }
    WkbParse::read(os, ctx);
}

void WkbParse::read(std::istream& is, WkbParseContext& ctx) {
    is.seekg(0, std::ios::end);
    auto size = is.tellg();
    is.seekg(0, std::ios::beg);

    // Check if size is valid
    if (size <= 0) {
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    std::vector<unsigned char> buf(static_cast<size_t>(size));
    if (!is.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(size))) {
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    ctx.dis = ByteOrderDataInStream(buf.data(), buf.size());
    std::unique_ptr<GeoShape> shape = readGeometry(ctx);
    if (!shape || ctx.dis.size() != 0) {
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
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

        const auto byte_order = ctx.dis.readByte();
        if (byte_order == byteOrder::wkbNDR) {
            ctx.dis.setOrder(ByteOrderValues::ENDIAN_LITTLE);
        } else if (byte_order == byteOrder::wkbXDR) {
            ctx.dis.setOrder(ByteOrderValues::ENDIAN_BIG);
        } else {
            return nullptr;
        }

        const uint32_t type_int = ctx.dis.readUnsigned();
        const uint32_t iso_type = type_int & ~WKB_EWKB_FLAGS;
        const uint32_t iso_dimension = iso_type / 1000;
        if (iso_dimension > 3) {
            return nullptr;
        }
        const uint32_t geometry_type = iso_type % 1000;

        const bool has_z = iso_dimension == 1 || iso_dimension == 3 || (type_int & WKB_Z_FLAG) != 0;
        const bool has_m = iso_dimension == 2 || iso_dimension == 3 || (type_int & WKB_M_FLAG) != 0;
        ctx.inputDimension = 2 + has_z + has_m;
        ctx.coordinate_type = has_z ? (has_m ? GeoCoordinateType::XYZM : GeoCoordinateType::XYZ)
                                    : (has_m ? GeoCoordinateType::XYM : GeoCoordinateType::XY);

        if ((type_int & WKB_SRID_FLAG) != 0) {
            if (ctx.dis.size() < sizeof(uint32_t)) {
                return nullptr;
            }
            ctx.srid = ctx.dis.readUnsigned();
        }

        std::unique_ptr<GeoShape> shape;

        switch (geometry_type) {
        case wkbType::wkbPoint:
            shape = readPoint(ctx);
            break;
        case wkbType::wkbLine:
            shape = readLine(ctx);
            break;
        case wkbType::wkbPolygon:
            shape = readPolygon(ctx);
            break;
        case wkbType::wkbMultiPolygon:
            shape = readMultiPolygon(ctx);
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
    if (!point || point->from_coord(coords.list[0]) != GEO_PARSE_OK) {
        return nullptr;
    }

    return point;
}

std::unique_ptr<GeoLine> WkbParse::readLine(WkbParseContext& ctx) {
    if (ctx.dis.size() < sizeof(uint32_t)) {
        return nullptr;
    }
    uint32_t size = ctx.dis.readUnsigned();
    if (minMemSize(wkbLine, size, ctx) != GEO_PARSE_OK) {
        return nullptr;
    }

    GeoCoordinateList coords = WkbParse::readCoordinateList(size, ctx);
    if (coords.list.empty()) {
        return nullptr;
    }

    std::unique_ptr<GeoLine> line = GeoLine::create_unique();
    if (!line || line->from_coords(coords) != GEO_PARSE_OK) {
        return nullptr;
    }

    return line;
}

std::unique_ptr<GeoPolygon> WkbParse::readPolygon(WkbParseContext& ctx) {
    if (ctx.dis.size() < sizeof(uint32_t)) {
        return nullptr;
    }
    uint32_t num_loops = ctx.dis.readUnsigned();
    if (minMemSize(wkbPolygon, num_loops, ctx) != GEO_PARSE_OK) {
        return nullptr;
    }

    GeoCoordinateListList coordss;
    for (uint32_t i = 0; i < num_loops; ++i) {
        if (ctx.dis.size() < sizeof(uint32_t)) {
            return nullptr;
        }
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
    if (!polygon || polygon->from_coords(coordss) != GEO_PARSE_OK) {
        return nullptr;
    }

    return polygon;
}

std::unique_ptr<GeoMultiPolygon> WkbParse::readMultiPolygon(WkbParseContext& ctx) {
    if (ctx.dis.size() < sizeof(uint32_t)) {
        return nullptr;
    }
    const auto coordinate_type = ctx.coordinate_type;
    const uint32_t polygon_count = ctx.dis.readUnsigned();
    if (polygon_count > ctx.dis.size() / 5) {
        return nullptr;
    }

    std::vector<GeoCoordinateListList> coordinates;
    coordinates.reserve(polygon_count);
    for (uint32_t i = 0; i < polygon_count; ++i) {
        auto shape = readGeometry(ctx);
        if (!shape || shape->type() != GEO_SHAPE_POLYGON ||
            shape->coordinate_type() != coordinate_type) {
            return nullptr;
        }
        auto* polygon = static_cast<GeoPolygon*>(shape.get());
        coordinates.push_back(std::move(*polygon->to_coords()));
    }
    ctx.coordinate_type = coordinate_type;
    ctx.inputDimension = 2 +
                         (coordinate_type == GeoCoordinateType::XYZ ||
                          coordinate_type == GeoCoordinateType::XYZM) +
                         (coordinate_type == GeoCoordinateType::XYM ||
                          coordinate_type == GeoCoordinateType::XYZM);

    auto multi_polygon = GeoMultiPolygon::create_unique();
    if (!multi_polygon || multi_polygon->from_coords(coordinates) != GEO_PARSE_OK) {
        return nullptr;
    }
    return multi_polygon;
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
        coord.type = ctx.coordinate_type;
        if (coord.has_z()) {
            coord.z = ctx.ordValues[j++];
        }
        if (coord.has_m()) {
            coord.m = ctx.ordValues[j++];
        }
        coords.add(coord);
    }
    return coords;
}

GeoParseStatus WkbParse::minMemSize(int wkbType, uint64_t size, WkbParseContext& ctx) {
    uint64_t minSize = 0;
    const uint64_t minCoordSize = ctx.inputDimension * sizeof(double);
    //constexpr uint64_t minPtSize = (1+4) + minCoordSize;
    //constexpr uint64_t minLineSize = (1+4+4); // empty line
    constexpr uint64_t minLoopSize = 4; // empty loop
    //constexpr uint64_t minPolySize = (1+4+4); // empty polygon
    //constexpr uint64_t minGeomSize = minLineSize;

    switch (wkbType) {
    case wkbLine:
        minSize = size * minCoordSize;
        break;
    case wkbPolygon:
        minSize = size * minLoopSize;
        break;
    }
    if (ctx.dis.size() < minSize) {
        return GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return GEO_PARSE_OK;
}
bool WkbParse::readCoordinate(WkbParseContext& ctx) {
    if (ctx.inputDimension > ctx.ordValues.size() ||
        ctx.dis.size() < ctx.inputDimension * sizeof(double)) {
        return false;
    }
    for (std::size_t i = 0; i < ctx.inputDimension; ++i) {
        ctx.ordValues[i] = ctx.dis.readDouble();
    }

    return true;
}

} // namespace doris
