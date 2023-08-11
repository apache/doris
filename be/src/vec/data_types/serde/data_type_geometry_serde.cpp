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

#include "data_type_geometry_serde.h"
#include "geo/util/GeoShape.h"
namespace doris {
namespace vectorized {
    template <bool is_binary_format>
    Status DataTypeGeometrySerDe::_write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<is_binary_format>& result,
                                                      int row_idx, bool col_const) const {
        auto& data = assert_cast<const ColumnString&>(column);
        const auto col_index = index_check_const(row_idx, col_const);
        const auto geo_val = data.get_data_at(col_index);
        // geo size == 0 is NULL
        if (geo_val.data == nullptr || geo_val.size == 0) {
            if (UNLIKELY(0 != result.push_null())) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            std::unique_ptr<GeoShape> shape;
            std::string string_temp;
            size_t data_size = geo_val.size;
            shape.reset(GeoShape::from_encoded(geo_val.data, data_size));
            if(shape != nullptr ){
                string_temp = shape->as_wkt();
            }
            if (UNLIKELY(0 != result.push_string(string_temp.c_str(), string_temp.size()))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }
        return Status::OK();
    }

    Status DataTypeGeometrySerDe::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                     bool col_const) const {
        return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
    }

    Status DataTypeGeometrySerDe::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                     bool col_const) const {
        return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
    }

} // namespace vectorized
} // namespace doris
