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

#include <string>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/wrapper_field.h"

namespace doris {
namespace segment_v2 {

// for extend zone map in the future
enum ZoneMapVersion {
    ZONE_MAP_V1
};

// Zone Map store the statistics info, V1 include:
//  min value
//  max value
//  is all null value
class ZoneMap {
public:
    ZoneMap();

    ZoneMap(ZoneMapVersion version);

    ~ZoneMap() { }

    Status serialize(std::string* dst) const;

    // this api will create min_value and max_value
    Status deserialize(const std::string& src);

    // lower bound for the values of the page
    const std::string& min_value() const {
        return _min_value;
    }

    void set_min_value(const std::string& min_value) {
        _min_value = min_value;
    }

    // upper bound for the values of the page
    const std::string& max_value() const {
        return _max_value;
    }

    void set_max_value(const std::string& max_value) {
        _max_value = max_value;
    }

    /**
    * bool value to determine the validity of the corresponding
    * min and max value. If true, a page contains only null values, and writers
    * have to set the corresponding entries in min_values and max_values to "",
    * so that all lists have the same length. If false, the
    * corresponding entry in min_value and max_value must be valid.
    */
    bool is_null() {
        return _is_null;
    }

    void set_is_null(bool is_null) {
        _is_null = is_null;
    }

private:
    ZoneMapVersion _version;
    std::string _min_value;
    std::string _max_value;
    bool _is_null;
};

} // namespace segment_v2
} // namespace doris
