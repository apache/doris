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

#include "geometry_value.h"

#include "geo/util/GeoShape.h"

namespace doris {

Status GeometryBinaryValue::from_geometry_string(const char* s, int length) {
    GeoParseStatus status;
    std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(s, length, &status));
    if (shape == nullptr || status != GEO_PARSE_OK) {
        return Status::InvalidArgument("geometry parse error: {} for value: {}", "Invalid WKT data",
                                       std::string_view(s, length));
    }

    std::string res;
    shape->encode_to(&res);
    len = res.size();
    ptr = new char[len + 1];
    std::copy(res.begin(), res.end(), const_cast<char*>(ptr));
    const_cast<char*>(ptr)[len] = '\0';
    DCHECK_LE(len, MAX_LENGTH);
    return Status::OK();
}

std::string GeometryBinaryValue::to_geometry_string() const {
    std::string new_str(ptr, len);
    return new_str;
}

} // namespace doris
