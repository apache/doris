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

//#include "geo_types.h"

#include <string>

struct ToBinaryContext;

namespace doris {

class GeoShape;
class GeoPoint;
class GeoLine;
class GeoPolygon;
struct GeoCoordinate;
struct GeoCoordinateList;

class toBinary {
public:
    static bool geo_tobinary(GeoShape* shape, std::string* result);

    static bool write(GeoShape* shape, ToBinaryContext* ctx);

private:
    static bool writeGeoPoint(GeoPoint* point, ToBinaryContext* ctx);

    static bool writeGeoLine(GeoLine* line, ToBinaryContext* ctx);

    static bool writeGeoPolygon(GeoPolygon* polygon, ToBinaryContext* ctx);

    static void writeByteOrder(ToBinaryContext* ctx);

    static void writeGeometryType(int geometryType, ToBinaryContext* ctx);

    static void writeInt(int intValue, ToBinaryContext* ctx);

    static void writeCoordinateList(const GeoCoordinateList& coords, bool sized,
                                    ToBinaryContext* ctx);

    static void writeCoordinate(GeoCoordinate& coords, ToBinaryContext* ctx);
};

} // namespace doris
