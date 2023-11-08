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

#include <cmath>
#include <cstddef>
#include <istream>
#include <sstream>
#include <vector>

#include "geo/ByteOrderDataInStream.h"
#include "geo/ByteOrderValues.h"
#include "geo/wkb_parse_ctx.h"
#include "geo_tobinary_type.h"
#include "util/GeoCollection.h"
#include "util/GeoLineString.h"
#include "util/GeoMultiLineString.h"
#include "util/GeoMultiPoint.h"
#include "util/GeoMultiPolygon.h"
#include "util/GeoPoint.h"
#include "util/GeoPolygon.h"
#include "util/GeoShape.h"

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
        ctx.parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
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
    case wkbType::wkbMultiPoint:
        shape.reset(readMultiPoint(ctx).release());
        break;
    case wkbType::wkbMultiLineString:
        shape.reset(readMultiLineString(ctx).release());
        break;
    case wkbType::wkbMultiPolygon:
        shape.reset(readMultiPolygon(ctx).release());
        break;
    case wkbType::wkbGeometryCollection:
        shape.reset(read_geometry_collection(ctx).release());
        break;
    default:
        return nullptr;
    }
    return shape;
}

std::unique_ptr<GeoPoint> WkbParse::readPoint(WkbParseContext* ctx) {
    std::unique_ptr<GeoPoint> point = GeoPoint::create_unique();

    GeoCoordinates coords = WkbParse::readCoordinateList(1, ctx);
    //POINT EMPTY WKB representation
    if (std::isnan(coords.coords[0].x) && std::isnan(coords.coords[0].y)) {
        point->set_empty();
        return point;
    }

    if (point->from_coord(coords.coords[0]) == GEO_PARSE_OK) {
        return point;
    } else {
        return nullptr;
    }
}

std::unique_ptr<GeoLineString> WkbParse::readLine(WkbParseContext* ctx) {
    std::unique_ptr<GeoLineString> linestring = GeoLineString::create_unique();
    uint32_t size = ctx->dis.readUnsigned();

    //LINESTRING EMPTY
    if (size == 0) {
        linestring->set_empty();
        return linestring;
    }
    minMemSize(wkbLine, size, ctx);

    GeoCoordinates coords = WkbParse::readCoordinateList(size, ctx);
    if (linestring->from_coords(coords) == GEO_PARSE_OK) {
        return linestring;
    } else {
        return nullptr;
    }
}

std::unique_ptr<GeoPolygon> WkbParse::readPolygon(WkbParseContext* ctx) {
    std::unique_ptr<GeoPolygon> polygon = GeoPolygon::create_unique();
    uint32_t num_loops = ctx->dis.readUnsigned();

    //POLYGON EMPTY
    if (num_loops == 0) {
        polygon->set_empty();
        return polygon;
    }
    minMemSize(wkbPolygon, num_loops, ctx);
    GeoCoordinateLists coords_list;
    for (int i = 0; i < num_loops; ++i) {
        uint32_t size = ctx->dis.readUnsigned();
        GeoCoordinates* coords = new GeoCoordinates();
        *coords = WkbParse::readCoordinateList(size, ctx);
        coords_list.add(coords);
    }

    if (polygon->from_coords(coords_list) == GEO_PARSE_OK) {
        return polygon;
    } else {
        return nullptr;
    }
}

std::unique_ptr<GeoMultiPoint> WkbParse::readMultiPoint(WkbParseContext* ctx) {
    std::unique_ptr<GeoMultiPoint> multi_point = GeoMultiPoint::create_unique();
    uint32_t num_points = ctx->dis.readUnsigned();

    //MULTIPOINT EMPTY
    if (num_points == 0) {
        multi_point->set_empty();
        return multi_point;
    }
    minMemSize(wkbMultiPoint, num_points, ctx);
    for (uint32_t i = 0; i < num_points; i++) {
        auto status = multi_point->add_one_geometry(readGeometry(ctx).release());
        if (status != GEO_PARSE_OK) {
            return nullptr;
        }
    }
    return multi_point;
}

std::unique_ptr<GeoMultiLineString> WkbParse::readMultiLineString(WkbParseContext* ctx) {
    std::unique_ptr<GeoMultiLineString> multi_linestring = GeoMultiLineString::create_unique();
    uint32_t num_linestring = ctx->dis.readUnsigned();

    //MULTILINESTRING EMPTY
    if (num_linestring == 0) {
        multi_linestring->set_empty();
        return multi_linestring;
    }
    minMemSize(wkbMultiLineString, num_linestring, ctx);
    for (uint32_t i = 0; i < num_linestring; i++) {
        auto status = multi_linestring->add_one_geometry(readGeometry(ctx).release());
        if (status != GEO_PARSE_OK) {
            return nullptr;
        }
    }
    return multi_linestring;
}

std::unique_ptr<GeoMultiPolygon> WkbParse::readMultiPolygon(WkbParseContext* ctx) {
    std::unique_ptr<GeoMultiPolygon> multi_polygon = GeoMultiPolygon::create_unique();
    uint32_t num_polygon = ctx->dis.readUnsigned();

    //MULTIPOLYGON EMPTY
    if (num_polygon == 0) {
        multi_polygon->set_empty();
        return multi_polygon;
    }
    minMemSize(wkbMultiPolygon, num_polygon, ctx);

    for (uint32_t i = 0; i < num_polygon; i++) {
        auto status = multi_polygon->add_one_geometry(readGeometry(ctx).release());
        if (status != GEO_PARSE_OK) {
            return nullptr;
        }
    }
    return multi_polygon;
}

std::unique_ptr<GeoCollection> WkbParse::read_geometry_collection(WkbParseContext* ctx) {
    std::unique_ptr<GeoCollection> collection = GeoCollection::create_unique();
    uint32_t num_geometry = ctx->dis.readUnsigned();

    //GEOMETRYCOLLECTION EMPTY
    if (num_geometry == 0) {
        collection->set_empty();
        return collection;
    }
    minMemSize(wkbGeometryCollection, num_geometry, ctx);
    for (uint32_t i = 0; i < num_geometry; i++) {
        auto status = collection->add_one_geometry(readGeometry(ctx).release());
        if (status != GEO_PARSE_OK) {
            return nullptr;
        }
    }
    return collection;
}

GeoCoordinates WkbParse::readCoordinateList(unsigned size, WkbParseContext* ctx) {
    GeoCoordinates coords;
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
    constexpr uint64_t minPtSize = (1 + 4) + minCoordSize;
    constexpr uint64_t minLineSize = (1 + 4 + 4); // empty line
    constexpr uint64_t minLoopSize = 4;           // empty loop
    constexpr uint64_t minPolySize = (1 + 4 + 4); // empty polygon
    constexpr uint64_t minGeomSize = minLineSize;

    switch (wkbType) {
    case wkbLine:
        minSize = size * minCoordSize;
        break;
    case wkbPolygon:
        minSize = size * minLoopSize;
        break;
    case wkbMultiPoint:
        minSize = size * minPtSize;
        break;
    case wkbMultiLineString:
        minSize = size * minLineSize;
        break;
    case wkbMultiPolygon:
        minSize = size * minPolySize;
        break;
    case wkbGeometryCollection:
        minSize = size * minGeomSize;
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
