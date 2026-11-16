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
#include <memory>
#include <utility>
#include <vector>

// This file include
namespace doris {

enum class GeoCoordinateType : uint8_t {
    XY = 0,
    XYZ = 1,
    XYM = 2,
    XYZM = 3,
    UNKNOWN = 0xff,
};

struct GeoCoordinate {
    double x = 0;
    double y = 0;
    double z = 0;
    double m = 0;
    GeoCoordinateType type = GeoCoordinateType::XY;

    bool has_z() const { return type == GeoCoordinateType::XYZ || type == GeoCoordinateType::XYZM; }
    bool has_m() const { return type == GeoCoordinateType::XYM || type == GeoCoordinateType::XYZM; }

    bool can_apply_declared_type(GeoCoordinateType declared_type) const {
        switch (declared_type) {
        case GeoCoordinateType::UNKNOWN:
            return type != GeoCoordinateType::UNKNOWN;
        case GeoCoordinateType::XYZ:
            return type == GeoCoordinateType::XYZ;
        case GeoCoordinateType::XYM:
            return type == GeoCoordinateType::XYZ;
        case GeoCoordinateType::XYZM:
            return type == GeoCoordinateType::XYZM;
        case GeoCoordinateType::XY:
            return type == GeoCoordinateType::XY;
        }
        return false;
    }

    void apply_declared_type(GeoCoordinateType declared_type) {
        if (declared_type == GeoCoordinateType::XYM) {
            m = z;
            z = 0;
            type = GeoCoordinateType::XYM;
        }
    }
};

struct GeoCoordinateList {
    void add(const GeoCoordinate& coordinate) { list.push_back(coordinate); }

    GeoCoordinateType coordinate_type() const {
        if (list.empty()) {
            return GeoCoordinateType::UNKNOWN;
        }
        const GeoCoordinateType type = list.front().type;
        for (const auto& coordinate : list) {
            if (coordinate.type != type) {
                return GeoCoordinateType::UNKNOWN;
            }
        }
        return type;
    }

    bool apply_declared_type(GeoCoordinateType declared_type) {
        for (const auto& coordinate : list) {
            if (!coordinate.can_apply_declared_type(declared_type)) {
                return false;
            }
        }
        for (auto& coordinate : list) {
            coordinate.apply_declared_type(declared_type);
        }
        return coordinate_type() != GeoCoordinateType::UNKNOWN;
    }

    std::vector<GeoCoordinate> list;
};

struct GeoCoordinateListList {
    GeoCoordinateListList() = default;
    GeoCoordinateListList(GeoCoordinateListList&& other) = default;
    GeoCoordinateListList& operator=(GeoCoordinateListList&& other) = default;

    GeoCoordinateListList(const GeoCoordinateListList& other) {
        for (const auto& coordinates : other.list) {
            list.emplace_back(std::make_unique<GeoCoordinateList>(*coordinates));
        }
    }

    GeoCoordinateListList& operator=(const GeoCoordinateListList& other) {
        if (this != &other) {
            GeoCoordinateListList copy(other);
            list.swap(copy.list);
        }
        return *this;
    }

    void add(std::unique_ptr<GeoCoordinateList>&& coordinates) {
        list.emplace_back(std::move(coordinates));
    }

    GeoCoordinateType coordinate_type() const {
        if (list.empty()) {
            return GeoCoordinateType::UNKNOWN;
        }
        const GeoCoordinateType type = list.front()->coordinate_type();
        if (type == GeoCoordinateType::UNKNOWN) {
            return type;
        }
        for (const auto& coordinates : list) {
            if (coordinates->coordinate_type() != type) {
                return GeoCoordinateType::UNKNOWN;
            }
        }
        return type;
    }

    bool apply_declared_type(GeoCoordinateType declared_type) {
        for (const auto& coordinates : list) {
            for (const auto& coordinate : coordinates->list) {
                if (!coordinate.can_apply_declared_type(declared_type)) {
                    return false;
                }
            }
        }
        for (auto& coordinates : list) {
            for (auto& coordinate : coordinates->list) {
                coordinate.apply_declared_type(declared_type);
            }
        }
        return coordinate_type() != GeoCoordinateType::UNKNOWN;
    }

    std::vector<std::unique_ptr<GeoCoordinateList>> list;
};

} // namespace doris
