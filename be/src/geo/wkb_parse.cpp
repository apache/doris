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

#include "wkb_parse.h"

#include <cstddef>
#include <istream>
#include <sstream>
#include <vector>

#include "geo/ByteOrderDataInStream.h"
#include "geo/ByteOrderValues.h"
#include "geo/geo_types.h"
#include "geo/wkb_parse_ctx.h"
#include "geo_tobinary_type.h"

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

GeoParseStatus WkbParse::parse_wkb(std::istream& is, GeoShape** shape) {
    WkbParseContext ctx;

    ctx = *(WkbParse::read_hex(is, &ctx));
    if (ctx.parse_status == GEO_PARSE_OK) {
        *shape = ctx.shape;
    } else {
        ctx.parse_status = GEO_PARSE_WKT_SYNTAX_ERROR;
    }
    return ctx.parse_status;
}

WkbParseContext* WkbParse::read_hex(std::istream& is, WkbParseContext* ctx) {
    // setup input/output stream
    std::stringstream os(std::ios_base::binary | std::ios_base::in | std::ios_base::out);

    while (true) {
        const int input_high = is.get();
        if (input_high == std::char_traits<char>::eof()) {
            break;
        }

        const int input_low = is.get();
        if (input_low == std::char_traits<char>::eof()) {
            ctx->parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
            return ctx;
        }

        const char high = static_cast<char>(input_high);
        const char low = static_cast<char>(input_low);

        const unsigned char result_high = ASCIIHexToUChar(high);
        const unsigned char result_low = ASCIIHexToUChar(low);

        const unsigned char value = static_cast<unsigned char>((result_high << 4) + result_low);

        // write the value to the output stream
        os << value;
    }
    return WkbParse::read(os, ctx);
}

WkbParseContext* WkbParse::read(std::istream& is, WkbParseContext* ctx) {
    is.seekg(0, std::ios::end);
    auto size = is.tellg();
    is.seekg(0, std::ios::beg);

    std::vector<unsigned char> buf(static_cast<size_t>(size));
    is.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(size));

    ctx->dis = ByteOrderDataInStream(buf.data(), buf.size()); // will default to machine endian

    ctx->shape = readGeometry(ctx).release();

    if (!ctx->shape) {
        ctx->parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return ctx;
}

std::unique_ptr<GeoShape> WkbParse::readGeometry(WkbParseContext* ctx) {
    // determine byte order
    unsigned char byteOrder = ctx->dis.readByte();

    // default is machine endian
    if (byteOrder == byteOrder::wkbNDR) {
        ctx->dis.setOrder(ByteOrderValues::ENDIAN_LITTLE);
    } else if (byteOrder == byteOrder::wkbXDR) {
        ctx->dis.setOrder(ByteOrderValues::ENDIAN_BIG);
    }

    uint32_t typeInt = ctx->dis.readUnsigned();

    uint32_t geometryType = (typeInt & 0xffff) % 1000;

    std::unique_ptr<GeoShape> shape;

    switch (geometryType) {
    case wkbType::wkbPoint:
        shape.reset(readPoint(ctx).release());
        break;
    case wkbType::wkbLine:
        shape.reset(readLine(ctx).release());
        break;
    case wkbType::wkbPolygon:
        shape.reset(readPolygon(ctx).release());
        break;
    default:
        return nullptr;
    }
    return shape;
}

std::unique_ptr<GeoPoint> WkbParse::readPoint(WkbParseContext* ctx) {
    GeoCoordinateList coords = WkbParse::readCoordinateList(1, ctx);
    std::unique_ptr<GeoPoint> point = GeoPoint::create_unique();

    if (point->from_coord(coords.list[0]) == GEO_PARSE_OK) {
        return point;
    } else {
        return nullptr;
    }
}

std::unique_ptr<GeoLine> WkbParse::readLine(WkbParseContext* ctx) {
    uint32_t size = ctx->dis.readUnsigned();
    minMemSize(wkbLine, size, ctx);

    GeoCoordinateList coords = WkbParse::readCoordinateList(size, ctx);
    std::unique_ptr<GeoLine> line = GeoLine::create_unique();

    if (line->from_coords(coords) == GEO_PARSE_OK) {
        return line;
    } else {
        return nullptr;
    }
}

std::unique_ptr<GeoPolygon> WkbParse::readPolygon(WkbParseContext* ctx) {
    uint32_t num_loops = ctx->dis.readUnsigned();
    minMemSize(wkbPolygon, num_loops, ctx);
    GeoCoordinateListList coordss;
    for (int i = 0; i < num_loops; ++i) {
        uint32_t size = ctx->dis.readUnsigned();
        GeoCoordinateList* coords = new GeoCoordinateList();
        *coords = WkbParse::readCoordinateList(size, ctx);
        coordss.add(coords);
    }

    std::unique_ptr<GeoPolygon> polygon = GeoPolygon::create_unique();

    if (polygon->from_coords(coordss) == GEO_PARSE_OK) {
        return polygon;
    } else {
        return nullptr;
    }
}

GeoCoordinateList WkbParse::readCoordinateList(unsigned size, WkbParseContext* ctx) {
    GeoCoordinateList coords;
    for (uint32_t i = 0; i < size; i++) {
        readCoordinate(ctx);
        unsigned int j = 0;
        GeoCoordinate coord;
        coord.x = ctx->ordValues[j++];
        coord.y = ctx->ordValues[j++];
        coords.add(coord);
    }

    return coords;
}

GeoParseStatus WkbParse::minMemSize(int wkbType, uint64_t size, WkbParseContext* ctx) {
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
    if (ctx->dis.size() < minSize) {
        return GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return GEO_PARSE_OK;
}
bool WkbParse::readCoordinate(WkbParseContext* ctx) {
    for (std::size_t i = 0; i < ctx->inputDimension; ++i) {
        ctx->ordValues[i] = ctx->dis.readDouble();
    }

    return true;
}

} // namespace doris
