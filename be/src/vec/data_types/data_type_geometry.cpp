//
// Created by Lemon on 2023/4/18.
//

#include "data_type_geometry.h"
#include "vec/columns/column_const.h"
#include "geo/geo_types.h"

namespace doris::vectorized {

std::string DataTypeGeometry::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    GeoParseStatus status;
    const StringRef& s = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    std::unique_ptr<GeoShape> shape = nullptr;
    if (s.size > 0){
        shape.reset(GeoShape::from_wkb(s.data, s.size, &status));
    } else {
        return "";
    }
    return shape->as_wkt();
}

void DataTypeGeometry::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                              class doris::vectorized::BufferWritable& ostr) const {
    std::string str = to_string(column, row_num);
    ostr.write(str.c_str(), str.size());
}

Status DataTypeGeometry::from_string(ReadBuffer& rb, IColumn* column) const {
    GeometryBinaryValue value;
    RETURN_IF_ERROR(value.from_geometry_string(rb.position(), rb.count()));

    auto* column_string = static_cast<ColumnString*>(column);
    column_string->insert_data(value.value(), value.size());

    return Status::OK();
}

MutableColumnPtr DataTypeGeometry::create_column() const {
    return ColumnString::create();
}

bool DataTypeGeometry::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

int64_t DataTypeGeometry::get_uncompressed_serialized_bytes(const IColumn& column,
                                                         int data_version) const {
    return data_type_string.get_uncompressed_serialized_bytes(column, data_version);
}

char* DataTypeGeometry::serialize(const IColumn& column, char* buf, int data_version) const {
    return data_type_string.serialize(column, buf, data_version);
}

const char* DataTypeGeometry::deserialize(const char* buf, IColumn* column, int data_version) const {
    return data_type_string.deserialize(buf, column, data_version);
}


}
