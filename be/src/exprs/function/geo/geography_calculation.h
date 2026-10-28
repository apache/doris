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

#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "core/string_ref.h"

namespace doris {
class GeoShape;
// The calculation domain is default spherical Geography XY Point/LineString/Polygon.
std::unique_ptr<GeoShape> decode_geography_calculation_shape(StringRef value);
Status geography_relation(std::string_view name, const GeoShape& lhs, const GeoShape& rhs,
                          StringRef left_wkb, StringRef right_wkb, bool* result);
bool geography_bearing(StringRef from, StringRef to, double* bearing);
double geography_distance(const GeoShape& left_shape, const GeoShape& right_shape,
                          StringRef left_wkb, StringRef right_wkb);
double geography_area(const GeoShape& shape, StringRef wkb, bool square_meters);
#ifdef BE_TEST
size_t geography_calculation_edges_for_test(const std::string& left, const std::string& right,
                                            size_t* exact_proofs = nullptr,
                                            size_t* candidate_visits = nullptr);
#endif
} // namespace doris
