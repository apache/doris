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

#include <h3api.h>

#include <cmath>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionH3LatLngToCell : public IFunction {
public:
    static constexpr auto name = "h3_lat_lng_to_cell";
    static FunctionPtr create() { return std::make_shared<FunctionH3LatLngToCell>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeInt64>());
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t rows) const override {
        auto lat = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[0]).column);
        auto lng = ColumnView<TYPE_DOUBLE>::create(block.get_by_position(arguments[1]).column);
        auto resolution = ColumnView<TYPE_INT>::create(block.get_by_position(arguments[2]).column);
        auto values = ColumnInt64::create(rows, 0);
        auto nulls = ColumnUInt8::create(rows, 0);
        for (size_t row = 0; row < rows; ++row) {
            const double latitude = lat.value_at(row);
            const double longitude = lng.value_at(row);
            const int res = resolution.value_at(row);
            if (!std::isfinite(latitude) || !std::isfinite(longitude) || latitude < -90 ||
                latitude > 90 || longitude < -180 || longitude > 180 || res < 0 || res > 15) {
                nulls->get_data()[row] = 1;
                continue;
            }
            const LatLng coord {degsToRads(latitude), degsToRads(longitude)};
            H3Index cell = 0;
            if (latLngToCell(&coord, res, &cell) != E_SUCCESS) {
                nulls->get_data()[row] = 1;
                continue;
            }
            values->get_data()[row] = static_cast<Int64>(cell);
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(values), std::move(nulls)));
        return Status::OK();
    }
};

template <bool boundary>
class FunctionH3CellToCoordinates : public IFunction {
public:
    static constexpr auto name = boundary ? "h3_cell_to_boundary" : "h3_cell_to_lat_lng";
    static FunctionPtr create() { return std::make_shared<FunctionH3CellToCoordinates>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        DataTypePtr type =
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
        if constexpr (boundary) {
            type = std::make_shared<DataTypeArray>(make_nullable(type));
        }
        return make_nullable(type);
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t rows) const override {
        auto cells = ColumnView<TYPE_BIGINT>::create(block.get_by_position(arguments[0]).column);
        auto values = ColumnFloat64::create();
        auto point_offsets = ColumnArray::ColumnOffsets::create();
        auto row_offsets = ColumnArray::ColumnOffsets::create();
        auto nulls = ColumnUInt8::create(rows, 0);
        for (size_t row = 0; row < rows; ++row) {
            CellBoundary coords {};
            const auto cell = cells.value_at(row);
            bool valid = cell > 0 && isValidCell(static_cast<H3Index>(cell));
            if (valid) {
                if constexpr (boundary) {
                    valid = cellToBoundary(cell, &coords) == E_SUCCESS;
                } else {
                    valid = cellToLatLng(cell, &coords.verts[0]) == E_SUCCESS;
                    coords.numVerts = 1;
                }
            }
            if (!valid) {
                nulls->get_data()[row] = 1;
            } else {
                for (int vertex = 0; vertex < coords.numVerts; ++vertex) {
                    values->get_data().push_back(radsToDegs(coords.verts[vertex].lat));
                    values->get_data().push_back(radsToDegs(coords.verts[vertex].lng));
                    if constexpr (boundary) {
                        point_offsets->get_data().push_back(values->size());
                    }
                }
            }
            row_offsets->get_data().push_back(boundary ? point_offsets->size() : values->size());
        }
        auto element_nulls = ColumnUInt8::create(values->size(), 0);
        ColumnPtr data = ColumnNullable::create(std::move(values), std::move(element_nulls));
        if constexpr (boundary) {
            data = ColumnArray::create(data, std::move(point_offsets));
            data = ColumnNullable::create(data, ColumnUInt8::create(data->size(), 0));
        }
        data = ColumnArray::create(data, std::move(row_offsets));
        block.replace_by_position(result, ColumnNullable::create(data, std::move(nulls)));
        return Status::OK();
    }
};

void register_function_h3(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionH3LatLngToCell>();
    factory.register_function<FunctionH3CellToCoordinates<false>>();
    factory.register_function<FunctionH3CellToCoordinates<true>>();
}

} // namespace doris
