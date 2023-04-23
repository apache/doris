//
// Created by Lemon on 2023/4/18.
//



#ifndef DORIS_GEOMETRY_VALUE_H
#define DORIS_GEOMETRY_VALUE_H


#include "geo/geo_tobinary.h"
#include "common/status.h"


namespace doris {
struct GeometryBinaryValue {
    static const int MAX_LENGTH = (1 << 30);

    // default nullprt and size 0 for invalid or NULL value
    const char* ptr = nullptr;
    size_t len = 0;
    std::stringstream result_stream;

    GeometryBinaryValue() : ptr(nullptr), len(0) {}
    GeometryBinaryValue(const std::string& s,std::string& res) { from_geometry_string(s.c_str(), s.length(),res); }
    GeometryBinaryValue(const char* ptr, int len) { from_geometry_string(ptr, len); }

    const char* value() { return ptr; }

    size_t size() { return len; }

    void replace(char* ptr, int len) {
        this->ptr = ptr;
        this->len = len;
    }



    Status from_geometry_string(const char* s, int len,std::string& res);
    Status from_geometry_string(const char* s, int len);

    std::string to_geometry_string() const;

};
}


#endif //DORIS_GEOMETRY_VALUE_H
