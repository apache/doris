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

#include <vector>

// This file include
namespace doris {

struct GeoCoordinate {
    double x;
    double y;

    bool operator==(const GeoCoordinate& other) const {
        return x == other.x && y == other.y;
    }
};

struct GeoCoordinates {
    void add(const GeoCoordinate& coordinate) { coords.push_back(coordinate); }
    std::vector<GeoCoordinate> coords;
};

struct GeoCoordinateLists {
    ~GeoCoordinateLists() {
        for (auto item : coords_list) {
            delete item;
        }
    }
    void add(GeoCoordinates* coordinates) { coords_list.push_back(coordinates); }
    std::vector<GeoCoordinates*> coords_list;
};

struct GeoCoordinateListCollections {
    ~GeoCoordinateListCollections() {
        for (auto item : coords_list_collections) {
            delete item;
        }
    }
    void add(GeoCoordinateLists* coordinatelists) { coords_list_collections.push_back(coordinatelists); }
    std::vector<GeoCoordinateLists*> coords_list_collections;
};

struct CompareA {
    bool operator()(const GeoCoordinate& a1, const GeoCoordinate& a2) const {
        if (a1.x < a2.x) {
            return true;
        } else if (a1.x > a2.x) {
            return false;
        } else {
            return a1.y < a2.y;
        }
    }
};

} // namespace doris
