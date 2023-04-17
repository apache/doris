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

#include <array>
#include <cstddef>
#include <istream>
#include <vector>

#include "geo/ByteOrderDataInStream.h"
#include "geo/ByteOrderValues.h"
#include "geo/geo_types.h"
#include "geo/wkb_parse_ctx.h"
#include "geo_tobinary_type.h"

namespace doris {

GeoParseStatus WkbParse::parse_wkb(std::istream& is, bool isEwkb, GeoShape** shape) {
    WkbParseContext ctx;

    ctx.isEwkb = isEwkb;
    ctx = *(WkbParse::read(is, &ctx));
    if (ctx.parse_status == GEO_PARSE_OK) {
        *shape = ctx.shape;
    } else {
        ctx.parse_status = GEO_PARSE_WKT_SYNTAX_ERROR;
    }
    return ctx.parse_status;
}

WkbParseContext* WkbParse::read(std::istream& is, WkbParseContext* ctx) {
    is.seekg(0, std::ios::end);
    auto size = is.tellg();
    is.seekg(0, std::ios::beg);

    std::vector<unsigned char> buf(static_cast<size_t>(size));
    is.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(size));

    ctx->dis = ByteOrderDataInStream(buf.data(), buf.size()); // will default to machine endian

    ctx->shape = readGeometry(ctx);

    if (!ctx->shape) {
        ctx->parse_status = GEO_PARSE_WKB_SYNTAX_ERROR;
    }
    return ctx;
}

GeoShape* WkbParse::readGeometry(WkbParseContext* ctx) {
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

    bool hasSRID = ((typeInt & 0x20000000) != 0);

    if (ctx->isEwkb) {
        if (!hasSRID) {
            return nullptr;
        }

        ctx->srid = ctx->dis.readInt(); // read SRID
        if (ctx->srid != 4326) {
            return nullptr;
        }
    }

    if (!ctx->isEwkb && hasSRID) {
        return nullptr;
    }

    GeoShape* shape;

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
}

GeoPoint* WkbParse::readPoint(WkbParseContext* ctx) {
    GeoCoordinateList coords = WkbParse::readCoordinateList(1, ctx);
    GeoPoint* point = new GeoPoint();

    if (point->from_coord(coords.list[0]) == GEO_PARSE_OK) {
        return point;
    } else {
        return nullptr;
    }
}

GeoLine* WkbParse::readLine(WkbParseContext* ctx) {
    uint32_t size = ctx->dis.readUnsigned();
    minMemSize(wkbLine, size, ctx);

    GeoCoordinateList coords = WkbParse::readCoordinateList(size, ctx);
    GeoLine* line = new GeoLine();

    if (line->from_coords(coords) == GEO_PARSE_OK) {
        return line;
    } else {
        return nullptr;
    }
}

GeoPolygon* WkbParse::readPolygon(WkbParseContext* ctx) {
    uint32_t num_loops = ctx->dis.readUnsigned();
    minMemSize(wkbPolygon, num_loops, ctx);
    GeoCoordinateListList coordss;
    for (int i = 0; i < num_loops; ++i) {
        uint32_t size = ctx->dis.readUnsigned();
        GeoCoordinateList* coords = new GeoCoordinateList();
        *coords = WkbParse::readCoordinateList(size, ctx);
        coordss.add(coords);
    }

    GeoPolygon* polygon = new GeoPolygon();

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
