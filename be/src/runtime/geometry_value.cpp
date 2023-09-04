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

Status GeometryBinaryValue::from_geometry_string(const char* s, size_t length) {
    size_t a = length;
    std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(s, a));
    if (shape == nullptr) {
        return Status::InvalidArgument("geometry parse error for value: {}", "Invalid data",
                                       std::string_view(s, length));
    }
    len = length;
    ptr = s;
    DCHECK_LE(len, MAX_LENGTH);
    return Status::OK();
}

std::string GeometryBinaryValue::to_geometry_string() const {
    std::string new_str(ptr, len);
    return new_str;
}

} // namespace doris
