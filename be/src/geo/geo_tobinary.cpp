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

#include "geo_tobinary.h"

#include <cstddef>
#include <sstream>
#include <vector>

#include "geo/ByteOrderValues.h"
#include "geo/geo_common.h"
#include "geo/machine.h"
#include "geo/wkt_parse_type.h"
#include "geo_tobinary_type.h"
#include "geo_types.h"
#include "iomanip"

namespace doris {

bool toBinary::geo_tobinary(GeoShape* shape, std::string* result) {
    ToBinaryContext ctx;
    std::stringstream result_stream;
    ctx.outStream = &result_stream;
    if (toBinary::write(shape, &ctx)) {
        std::stringstream hex_stream;
        hex_stream << std::hex << std::setfill('0');
        result_stream.seekg(0);
        unsigned char c;
        while (result_stream.read(reinterpret_cast<char*>(&c), 1)) {
            hex_stream << std::setw(2) << static_cast<int>(c);
        }
        //for compatibility with postgres
        *result = "\\x" + hex_stream.str();
        return true;
    }
    return false;
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
    default:
        return false;
    }
}

bool toBinary::writeGeoPoint(GeoPoint* point, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbPoint, ctx);
    GeoCoordinateList p = point->to_coords();

    writeCoordinateList(p, false, ctx);
    return true;
}

bool toBinary::writeGeoLine(GeoLine* line, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbLine, ctx);
    GeoCoordinateList p = line->to_coords();

    writeCoordinateList(p, true, ctx);
    return true;
}

bool toBinary::writeGeoPolygon(doris::GeoPolygon* polygon, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbPolygon, ctx);
    writeInt(polygon->numLoops(), ctx);
    std::unique_ptr<GeoCoordinateListList> coordss(polygon->to_coords());

    for (int i = 0; i < coordss->list.size(); ++i) {
        writeCoordinateList(*coordss->list[i], true, ctx);
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

void toBinary::writeGeometryType(int typeId, ToBinaryContext* ctx) {
    writeInt(typeId, ctx);
}

void toBinary::writeInt(int val, ToBinaryContext* ctx) {
    ByteOrderValues::putInt(val, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 4);
}

void toBinary::writeCoordinateList(const GeoCoordinateList& coords, bool sized,
                                   ToBinaryContext* ctx) {
    std::size_t size = coords.list.size();

    if (sized) {
        writeInt(static_cast<int>(size), ctx);
    }
    for (std::size_t i = 0; i < size; i++) {
        GeoCoordinate coord = coords.list[i];
        writeCoordinate(coord, ctx);
    }
}

void toBinary::writeCoordinate(GeoCoordinate& coords, ToBinaryContext* ctx) {
    ByteOrderValues::putDouble(coords.x, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 8);
    ByteOrderValues::putDouble(coords.y, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 8);
}

} // namespace doris
