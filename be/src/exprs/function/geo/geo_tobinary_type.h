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

namespace doris {

enum byteOrder { wkbXDR = 0, wkbNDR = 1 };

enum wkbType {
    wkbPoint = 1,
    wkbLine = 2,
    wkbPolygon = 3,
    wkbMultiPoint = 4,
    wkbMultiLineString = 5,
    wkbMultiPolygon = 6,
    wkbGeometryCollection = 7
};

constexpr uint32_t WKB_Z_FLAG = 0x80000000;
constexpr uint32_t WKB_M_FLAG = 0x40000000;
constexpr uint32_t WKB_SRID_FLAG = 0x20000000;
constexpr uint32_t WKB_EWKB_FLAGS = WKB_Z_FLAG | WKB_M_FLAG | WKB_SRID_FLAG;

} // namespace doris

struct ToBinaryContext {
    // WKBConstants::wkbwkbXDR | WKBConstants::wkbNDR
    int byteOrder;
    bool ewkb = false;

    unsigned char buf[8];

    std::ostream* outStream = nullptr;
};
