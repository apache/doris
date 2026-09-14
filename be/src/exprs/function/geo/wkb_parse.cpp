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
        return GEO_PARSE_WKB_SYNTAX_ERROR;
    }
}

GeoParseStatus WkbParse::parse_wkb(std::istream& is, std::unique_ptr<GeoShape>& shape) {
    WkbParseContext ctx;

    WkbParse::read_hex(is, ctx);
    if (ctx.parse_status == GEO_PARSE_OK) {
        shape = std::move(ctx.shape);
    }
    return ctx.parse_status;
}

GeoParseStatus WkbParse::parse_wkb_bytes(const char* data, size_t size,
                                         std::unique_ptr<GeoShape>& shape) {
    WkbParseContext ctx;
    std::istringstream wkb(std::string(data, size), std::ios_base::binary | std::ios_base::in);
    WkbParse::read(wkb, ctx);
    if (ctx.parse_status == GEO_PARSE_OK) {
        shape = std::move(ctx.shape);
    }
    return ctx.parse_status;
}

GeoParseStatus WkbParse::validate_wkb_bytes(const char* data, size_t size) {
    if (size == 0) {
        return GEO_PARSE_WKB_SYNTAX_ERROR;
    }

    try {
        const auto byte_order = static_cast<unsigned char>(data[0]);
        WkbParseContext ctx;
        if (byte_order == byteOrder::wkbNDR) {
            ctx.dis = ByteOrderDataInStream(reinterpret_cast<const unsigned char*>(data), size);
            ctx.dis.setOrder(ByteOrderValues::ENDIAN_LITTLE);
        } else if (byte_order == byteOrder::wkbXDR) {
            ctx.dis = ByteOrderDataInStream(reinterpret_cast<const unsigned char*>(data), size);
            ctx.dis.setOrder(ByteOrderValues::ENDIAN_BIG);
        } else {
            return GEO_PARSE_WKB_SYNTAX_ERROR;
        }
        return validate_geometry(ctx) && ctx.dis.size() == 0 ? GEO_PARSE_OK
                                                             : GEO_PARSE_WKB_SYNTAX_ERROR;
    } catch (...) {
        return GEO_PARSE_WKB_SYNTAX_ERROR;
    }
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
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    std::vector<unsigned char> buf(static_cast<size_t>(size));
    if (!is.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(size))) {
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    // Ensure we have at least one byte for byte order
    if (buf.empty()) {
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

    // First read the byte order using machine endian
    auto byteOrder = buf[0];

    // Create ByteOrderDataInStream with the correct byte order
    if (byteOrder == byteOrder::wkbNDR) {
        ctx.dis = ByteOrderDataInStream(buf.data(), buf.size());
        ctx.dis.setOrder(ByteOrderValues::ENDIAN_LITTLE);
    } else if (byteOrder == byteOrder::wkbXDR) {
        ctx.dis = ByteOrderDataInStream(buf.data(), buf.size());
        ctx.dis.setOrder(ByteOrderValues::ENDIAN_BIG);
    } else {
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
        return;
    }

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

        // Skip the byte order as we've already handled it
        ctx.dis.readByte();

        uint32_t typeInt = ctx.dis.readUnsigned();

        constexpr uint32_t ewkb_z_flag = 0x80000000;
        constexpr uint32_t ewkb_m_flag = 0x40000000;
        constexpr uint32_t ewkb_srid_flag = 0x20000000;
        constexpr uint32_t ewkb_metadata_flags = ewkb_z_flag | ewkb_m_flag | ewkb_srid_flag;
        if ((typeInt & ewkb_metadata_flags) != 0 || (typeInt >= 1000 && typeInt < 4000)) {
            return nullptr;
        }

        // Get the base geometry type
        uint32_t geometryType = typeInt & WKB_TYPE_MASK;

        std::unique_ptr<GeoShape> shape;

        switch (geometryType) {
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
    if (!point || point->from_coord(coords.list[0]) != GEO_PARSE_OK) {
        return nullptr;
    }

    return point;
}

std::unique_ptr<GeoLine> WkbParse::readLine(WkbParseContext& ctx) {
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
    uint32_t num_loops = ctx.dis.readUnsigned();
    if (minMemSize(wkbPolygon, num_loops, ctx) != GEO_PARSE_OK) {
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
    if (!polygon || polygon->from_coords(coordss) != GEO_PARSE_OK) {
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

GeoParseStatus WkbParse::minMemSize(int wkbType, uint64_t size, WkbParseContext& ctx) {
    uint64_t minSize = 0;
    constexpr uint64_t minCoordSize = 2 * sizeof(double);
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
    for (std::size_t i = 0; i < ctx.inputDimension; ++i) {
        ctx.ordValues[i] = ctx.dis.readDouble();
    }

    return true;
}

bool WkbParse::validate_geometry(WkbParseContext& ctx) {
    if (ctx.dis.size() < 5) {
        return false;
    }
    ctx.dis.readByte();
    const uint32_t type = ctx.dis.readUnsigned();
    constexpr uint32_t ewkb_metadata_flags = 0xE0000000;
    if ((type & ewkb_metadata_flags) != 0 || (type >= 1000 && type < 4000)) {
        return false;
    }

    switch (type & WKB_TYPE_MASK) {
    case wkbType::wkbPoint:
        return validate_coordinates(1, ctx);
    case wkbType::wkbLine: {
        const uint32_t size = ctx.dis.readUnsigned();
        return size > 0 && validate_coordinates(size, ctx);
    }
    case wkbType::wkbPolygon: {
        const uint32_t loops = ctx.dis.readUnsigned();
        if (loops == 0 || loops > ctx.dis.size() / sizeof(uint32_t)) {
            return false;
        }
        for (uint32_t loop = 0; loop < loops; ++loop) {
            const uint32_t size = ctx.dis.readUnsigned();
            if (size < 3 || !validate_coordinates(size, ctx)) {
                return false;
            }
        }
        return true;
    }
    default:
        return false;
    }
}

bool WkbParse::validate_coordinates(uint32_t size, WkbParseContext& ctx) {
    constexpr size_t coordinate_size = 2 * sizeof(double);
    if (size > ctx.dis.size() / coordinate_size) {
        return false;
    }
    for (uint32_t coordinate = 0; coordinate < size; ++coordinate) {
        ctx.dis.readDouble();
        ctx.dis.readDouble();
    }
    return true;
}

} // namespace doris
