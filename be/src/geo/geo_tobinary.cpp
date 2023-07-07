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
#include "util/GeoShape.h"
#include "util/GeoPoint.h"
#include "util/GeoLineString.h"
#include "util/GeoPolygon.h"
#include "util/GeoCollection.h"
#include "iomanip"

namespace doris {

bool toBinary::geo_tobinary(GeoShape* shape, std::string* result, int is_hex) {
    ToBinaryContext ctx;
    std::stringstream result_stream;
    ctx.outStream = &result_stream;
    if (toBinary::write(shape, &ctx)) {
        if (is_hex) {
            *result = to_hex(result_stream.str());
        } else {
            *result = result_stream.str();
        }
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
        return writeGeoLine((GeoLineString*)(shape), ctx);
    }
    case GEO_SHAPE_POLYGON: {
        return writeGeoPolygon((GeoPolygon*)(shape), ctx);
    }
    case GEO_SHAPE_MULTI_POINT: {
        return writeGeoCollection((GeoCollection*)(shape), wkbType::wkbMultiPoint, ctx);
    }
    case GEO_SHAPE_MULTI_LINE_STRING: {
        return writeGeoCollection((GeoCollection*)(shape), wkbType::wkbMultiLineString, ctx);
    }
    case GEO_SHAPE_MULTI_POLYGON: {
        return writeGeoCollection((GeoCollection*)(shape), wkbType::wkbMultiPolygon, ctx);
    }
    case GEO_SHAPE_GEOMETRY_COLLECTION: {
        return writeGeoCollection((GeoCollection*)(shape), wkbType::wkbGeometryCollection, ctx);
    }
    default:
        return false;
    }
}

bool toBinary::writeGeoPoint(GeoPoint* point, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbPoint, ctx);
    if(point->is_empty()){
        GeoCoordinate c(DoubleNotANumber,DoubleNotANumber);
        writeCoordinate(c,ctx);
    } else {
        GeoCoordinates p = point->to_coords();
        writeCoordinateList(p, false, ctx);
    }
    return true;
}

bool toBinary::writeGeoLine(GeoLineString* linestring, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbLine, ctx);
    if(linestring->is_empty()){
        writeInt(0,ctx);
    } else {
        GeoCoordinates p = linestring->to_coords();
        writeCoordinateList(p, true, ctx);
    }
    return true;
}

bool toBinary::writeGeoPolygon(doris::GeoPolygon* polygon, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbType::wkbPolygon, ctx);
    if(polygon->is_empty()){
        writeInt(0,ctx);
    } else {
        writeInt(polygon->num_loops(), ctx);
        std::unique_ptr<GeoCoordinateLists> coords_list(polygon->to_coords());
        for (int i = 0; i < coords_list->coords_list.size(); ++i) {
            writeCoordinateList(*coords_list->coords_list[i], true, ctx);
        }
    }
    return true;
}


bool toBinary::writeGeoCollection(doris::GeoCollection* collection, int wkbtype, ToBinaryContext* ctx) {
    writeByteOrder(ctx);
    writeGeometryType(wkbtype, ctx);
    if(collection->is_empty()){
        writeInt(0,ctx);
    } else {
        auto ngeoms = collection->get_num_geometries();
        writeInt(static_cast<int>(ngeoms), ctx);
        for(std::size_t i = 0; i < ngeoms; i++) {
            GeoShape* shape = collection->get_geometries_n(i);
            write(shape,ctx);
        }
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

void toBinary::writeCoordinateList(const GeoCoordinates& coords, bool sized,
                                   ToBinaryContext* ctx) {
    std::size_t size = coords.coords.size();

    if (sized) {
        writeInt(static_cast<int>(size), ctx);
    }
    for (std::size_t i = 0; i < size; i++) {
        GeoCoordinate coord = coords.coords[i];
        writeCoordinate(coord, ctx);
    }
}

void toBinary::writeCoordinate(GeoCoordinate& coords, ToBinaryContext* ctx) {
    ByteOrderValues::putDouble(coords.x, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 8);
    ByteOrderValues::putDouble(coords.y, ctx->buf, ctx->byteOrder);
    ctx->outStream->write(reinterpret_cast<char*>(ctx->buf), 8);
}
std::string toBinary::to_hex(std::string binary) {
    std::stringstream wkb_binary;
    const char* data = binary.data();

    for (int i = 0; i < binary.size(); ++i) {
        wkb_binary << *data;
        data++;
    }

    std::stringstream hex_stream;
    hex_stream << std::hex << std::setfill('0');
    wkb_binary.seekg(0);
    unsigned char c;
    while (wkb_binary.read(reinterpret_cast<char*>(&c), 1)) {
        hex_stream << std::setw(2) << static_cast<int>(c);
    }
    return hex_stream.str();
}

} // namespace doris
