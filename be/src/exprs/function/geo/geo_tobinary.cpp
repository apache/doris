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

#include "exprs/function/geo/geo_tobinary.h"

#include <cstddef>
#include <iomanip>
#include <sstream>

#include "exprs/function/geo/ByteOrderValues.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_tobinary_type.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/machine.h"
#include "exprs/function/geo/wkt_parse_type.h"

namespace doris {

bool toBinary::geo_tobinary(GeoShape* shape, std::string* result) {
    return encode(shape, false, result);
}

bool toBinary::geo_toewkb(GeoShape* shape, std::string* result) {
    return encode(shape, true, result);
}

bool toBinary::encode(GeoShape* shape, bool ewkb, std::string* result) {
    ToBinaryContext ctx;
    std::stringstream result_stream;
    ctx.outStream = &result_stream;
    ctx.ewkb = ewkb;
    if (!toBinary::write(shape, &ctx)) {
        return false;
    }

    std::stringstream hex_stream;
    hex_stream << std::hex << std::setfill('0');
    result_stream.seekg(0);
    unsigned char c;
    while (result_stream.read(reinterpret_cast<char*>(&c), 1)) {
        hex_stream << std::setw(2) << static_cast<int>(c);
    }
    // For compatibility with PostgreSQL bytea output.
    *result = "\\x" + hex_stream.str();
    return true;
}

bool toBinary::write(GeoShape* shape, ToBinaryContext* ctx) {
    switch (shape->type()) {
    case GEO_SHAPE_POINT: {
        return writeGeoPoint((GeoPoint*)(shape), ctx);
    }
    case GEO_SHAPE_LINE_STRING: {
        return writeGeoLine((GeoLine*)(shape), ctx);
    }
    case GEO_SHAPE_POLYGON: {
        return writeGeoPolygon((GeoPolygon*)(shape), ctx);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        return writeGeoMultiPolygon((GeoMultiPolygon*)(shape), ctx);
    }
    default:
        return false;
    }
}

bool toBinary::writeGeoPoint(GeoPoint* point, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbPoint, point, ctx);
    GeoCoordinateList p = point->to_coords();

    writeCoordinateList(p, false, ctx);
    return true;
}

bool toBinary::writeGeoLine(GeoLine* line, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbLine, line, ctx);
    GeoCoordinateList p = line->to_coords();

    writeCoordinateList(p, true, ctx);
    return true;
}

bool toBinary::writeGeoPolygon(doris::GeoPolygon* polygon, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbPolygon, polygon, ctx);
    writeInt(static_cast<uint32_t>(polygon->numLoops()), ctx);
    std::unique_ptr<GeoCoordinateListList> coordss(polygon->to_coords());

    for (const auto& coordinates : coordss->list) {
        writeCoordinateList(*coordinates, true, ctx);
    }
    return true;
}

bool toBinary::writeGeoMultiPolygon(GeoMultiPolygon* multi_polygon, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbMultiPolygon, multi_polygon, ctx);
    writeInt(static_cast<uint32_t>(multi_polygon->polygons().size()), ctx);
    for (const auto& polygon : multi_polygon->polygons()) {
        writeGeoPolygon(polygon.get(), ctx);
    }
    return true;
}

void toBinary::writeByteOrder(ToBinaryContext* ctx) {
    ctx->byteOrder = getMachineByteOrder();
    if (ctx->byteOrder == 1) {
        ctx->buf[0] = byteOrder::wkbNDR;
    } else {
        ctx->buf[0] = byteOrder::wkbXDR;
    }

    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 1);
}

void toBinary::writeGeometryType(uint32_t geometry_type, GeoShape* shape, ToBinaryContext* ctx) {
    if (ctx->ewkb) {
        if (shape->has_z()) {
            geometry_type |= WKB_Z_FLAG;
        }
        if (shape->has_m()) {
            geometry_type |= WKB_M_FLAG;
        }
    }
    writeInt(geometry_type, ctx);
}

void toBinary::writeInt(uint32_t value, ToBinaryContext* ctx) {
    ByteOrderValues::putUnsigned(value, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 4);
}

void toBinary::writeCoordinateList(const GeoCoordinateList& coords, bool sized,
                                   ToBinaryContext* ctx) {
    std::size_t size = coords.list.size();

    if (sized) {
        writeInt(static_cast<uint32_t>(size), ctx);
    }
    for (const auto& coordinate : coords.list) {
        writeCoordinate(coordinate, ctx);
    }
}

void toBinary::writeCoordinate(const GeoCoordinate& coordinate, ToBinaryContext* ctx) {
    ByteOrderValues::putDouble(coordinate.x, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), sizeof(double));
    ByteOrderValues::putDouble(coordinate.y, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), sizeof(double));
    if (ctx->ewkb && coordinate.has_z()) {
        ByteOrderValues::putDouble(coordinate.z, ctx->buf, ctx->byteOrder);
        ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), sizeof(double));
    }
    if (ctx->ewkb && coordinate.has_m()) {
        ByteOrderValues::putDouble(coordinate.m, ctx->buf, ctx->byteOrder);
        ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), sizeof(double));
    }
}

} // namespace doris
