//
// Created by Lemon on 2023/4/18.
//

#include "geometry_value.h"
#include "geo/geo_types.h"

namespace doris {

Status GeometryBinaryValue::from_geometry_string(const char* s, int length,std::string& res) {


    GeoParseStatus status;
    std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(s,length,&status));
    if (shape == nullptr || status != GEO_PARSE_OK ) {
        return Status::InvalidArgument("geometry parse error: {} for value: {}",
                                       "Invalid WKT data", std::string_view(s, length));
    }

    res = GeoShape::as_binary(shape.get());
    DCHECK_LE(len, MAX_LENGTH);
    return Status::OK();
}

Status GeometryBinaryValue::from_geometry_string(const char* s, int length) {


    GeoParseStatus status;
    std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(s,length,&status));
    if (shape == nullptr || status != GEO_PARSE_OK ) {
        return Status::InvalidArgument("geometry parse error: {} for value: {}",
                                       "Invalid WKT data", std::string_view(s, length));
    }

    std::string res = GeoShape::as_binary(shape.get());
    ptr = res.data();
    len = res.size();
    DCHECK_LE(len, MAX_LENGTH);
    return Status::OK();
}

std::string GeometryBinaryValue::to_geometry_string() const {
    std::string new_str(ptr, len);
    return new_str;
}

}
