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

#include "core/data_type/data_type_spatial.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <string_view>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_binary_serialization.h"
#include "core/data_type_serde/data_type_varbinary_serde.h"
#include "core/field.h"
#include "core/string_view.h"
#include "exprs/function/geo/wkb_parse.h"
#include "gen_cpp/data.pb.h"

namespace doris {

namespace {

std::string normalize_spatial_crs(std::string crs) {
    // Imported ogc:crs84 has default semantics; leave custom CRS/property references unchanged.
    constexpr std::string_view default_crs = "OGC:CRS84";
    if (std::ranges::equal(crs, default_crs, [](unsigned char left, unsigned char right) {
            return std::tolower(left) == std::tolower(right);
        })) {
        return std::string(default_crs);
    }
    return crs;
}

std::string normalize_spatial_algorithm(PrimitiveType type, std::string algorithm) {
    if (type == TYPE_GEOGRAPHY && algorithm.empty()) {
        return "spherical";
    }
    std::ranges::transform(algorithm, algorithm.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return algorithm;
}

class SpatialParquetSerDe final : public DataTypeVarbinarySerDeBase<ColumnSpatial> {
public:
    SpatialParquetSerDe(int nesting_level, bool geographic)
            : DataTypeVarbinarySerDeBase<ColumnSpatial>(nesting_level), _geographic(geographic) {}

    Status read_column_from_parquet(IColumn& column, ParquetDecodeSource& source,
                                    const ParquetDecodeContext& context, size_t num_values,
                                    ParquetMaterializationState& state) const override {
        const auto start = column.size();
        RETURN_IF_ERROR(DataTypeVarbinarySerDeBase<ColumnSpatial>::read_column_from_parquet(
                column, source, context, num_values, state));
        // Validate materialized values, not unselected dictionary entries. NULL runs
        // are appended by the reader outside the leaf SerDe.
        const auto& values = assert_cast<const ColumnSpatial&>(column).get_data();
        for (size_t row = start; row < values.size(); ++row) {
            const auto& value = values[row];
            RETURN_IF_ERROR(
                    WkbParse::validate_spatial_wkb(value.data(), value.size(), _geographic, row));
        }
        return Status::OK();
    }

private:
    const bool _geographic;
};

} // namespace

DataTypeSpatial::DataTypeSpatial(PrimitiveType primitive_type, std::string crs,
                                 std::string algorithm)
        : _primitive_type(primitive_type),
          _crs(normalize_spatial_crs(std::move(crs))),
          _algorithm(normalize_spatial_algorithm(primitive_type, std::move(algorithm))) {
    DCHECK(primitive_type == TYPE_GEOMETRY || primitive_type == TYPE_GEOGRAPHY);
    DCHECK(!_crs.empty());
    DCHECK(primitive_type == TYPE_GEOMETRY || !_algorithm.empty());
    DCHECK(primitive_type == TYPE_GEOGRAPHY || _algorithm.empty());
}

void DataTypeSpatial::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->set_spatial_crs(_crs);
    if (_primitive_type == TYPE_GEOGRAPHY) {
        col_meta->set_spatial_algorithm(_algorithm);
    }
}

DataTypeSerDeSPtr DataTypeSpatial::get_serde(int nesting_level) const {
    return std::make_shared<SpatialParquetSerDe>(nesting_level, requires_geographic_coordinates());
}

doris::FieldType DataTypeSpatial::get_storage_field_type() const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Spatial types are supported only by external Iceberg tables");
    return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
}

MutableColumnPtr DataTypeSpatial::create_column() const {
    return ColumnSpatial::create(_primitive_type);
}

Status DataTypeSpatial::check_column(const IColumn& column) const {
    const IColumn* nested_column = &column;
    if (is_column_const(column)) {
        nested_column = &assert_cast<const ColumnConst&>(column).get_data_column();
    }
    const auto* spatial = check_and_get_column<ColumnSpatial>(nested_column);
    if (spatial == nullptr || spatial->get_primitive_type() != _primitive_type) {
        return Status::InvalidArgument("Expected {} spatial column, got {}", get_name(),
                                       column.get_name());
    }
    return Status::OK();
}

int64_t DataTypeSpatial::get_uncompressed_serialized_bytes(const IColumn& column,
                                                           int be_exec_version) const {
    return get_binary_column_serialized_bytes<ColumnSpatial>(column);
}

char* DataTypeSpatial::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    return serialize_binary_column<ColumnSpatial>(column, buf);
}

const char* DataTypeSpatial::deserialize(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const {
    return deserialize_binary_column<ColumnSpatial>(buf, column);
}

Field DataTypeSpatial::get_field(const TExprNode& /* node */) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Spatial literals must be constructed from WKB by a spatial function");
}

FieldWithDataType DataTypeSpatial::get_field_with_data_type(const IColumn& column,
                                                            size_t row_num) const {
    const auto value = assert_cast<const ColumnSpatial&>(column).get_data_at(row_num);
    return FieldWithDataType {.field = Field::create_field<TYPE_VARBINARY>(StringView(value)),
                              .base_scalar_type_id = _primitive_type};
}

bool DataTypeSpatial::equals(const IDataType& rhs) const {
    const auto* other = dynamic_cast<const DataTypeSpatial*>(&rhs);
    return other != nullptr && _primitive_type == other->_primitive_type && _crs == other->_crs &&
           _algorithm == other->_algorithm;
}

void DataTypeSpatial::to_protobuf(PTypeDesc* /* ptype */, PTypeNode* /* node */,
                                  PScalarType* scalar_type) const {
    scalar_type->set_spatial_crs(_crs);
    if (_primitive_type == TYPE_GEOGRAPHY) {
        scalar_type->set_spatial_algorithm(_algorithm);
    }
}

#ifdef BE_TEST
void DataTypeSpatial::to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const {
    IDataType::to_thrift(thrift_type, node);
    node.scalar_type.__set_spatial_crs(_crs);
    if (_primitive_type == TYPE_GEOGRAPHY) {
        node.scalar_type.__set_spatial_algorithm(_algorithm);
    }
}
#endif

} // namespace doris
