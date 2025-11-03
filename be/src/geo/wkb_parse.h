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

#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>

#include "geo/geo_common.h"
#include "geo/wkt_parse_type.h"

struct WkbParseContext;

namespace doris {

class GeoShape;
class GeoLine;
class GeoPoint;
class GeoPolygon;

// WKB format constants
// According to OpenGIS Implementation Specification:
// The high bit of the type value is set to 1 if the WKB contains a SRID.
// Reference: OpenGIS Implementation Specification for Geographic information - Simple feature access - Part 1: Common architecture
// Bit mask to check if WKB contains SRID
constexpr uint32_t WKB_SRID_FLAG = 0x20000000;

// The geometry type is stored in the least significant byte of the type value
// Bit mask to extract the base geometry type
constexpr uint32_t WKB_TYPE_MASK = 0xFF;

class WkbParse {
public:
    static GeoParseStatus parse_wkb(std::istream& is, GeoShape** shape);

    static WkbParseContext* read_hex(std::istream& is, WkbParseContext* ctx);

    static WkbParseContext* read(std::istream& is, WkbParseContext* ctx);

    static std::unique_ptr<GeoShape> readGeometry(WkbParseContext* ctx);

private:
    static std::unique_ptr<GeoPoint> readPoint(WkbParseContext* ctx);

    static std::unique_ptr<GeoLine> readLine(WkbParseContext* ctx);

    static std::unique_ptr<GeoPolygon> readPolygon(WkbParseContext* ctx);

    static GeoCoordinateList readCoordinateList(unsigned size, WkbParseContext* ctx);

    static GeoParseStatus minMemSize(int wkbType, uint64_t size, WkbParseContext* ctx);

    static bool readCoordinate(WkbParseContext* ctx);
};

} // namespace doris
