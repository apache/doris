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

#include <cstddef>

#include "vec/columns/column.h"

namespace doris {

class SlotDescriptor;

// Helper class for dealing with text data, e.g., converting text data to
// numeric types, etc.
class TextConverter {
public:
    static constexpr char NULL_STR[3] = {'\\', 'N', '\0'};

    TextConverter(char escape_char, char collection_delimiter = '\2', char map_kv_delimiter = '\3');

    inline void write_string_column(const SlotDescriptor* slot_desc,
                                    vectorized::MutableColumnPtr* column_ptr, const char* data,
                                    size_t len) {
        return write_string_column(slot_desc, column_ptr, data, len, false);
    }

    void write_string_column(const SlotDescriptor* slot_desc,
                             vectorized::MutableColumnPtr* column_ptr, const char* data, size_t len,
                             bool need_escape);

    inline bool write_column(const SlotDescriptor* slot_desc,
                             vectorized::MutableColumnPtr* column_ptr, const char* data, size_t len,
                             bool copy_string, bool need_escape) {
        vectorized::IColumn* nullable_col_ptr = column_ptr->get();
        return write_vec_column(slot_desc, nullable_col_ptr, data, len, copy_string, need_escape);
    }

    inline bool write_vec_column(const SlotDescriptor* slot_desc,
                                 vectorized::IColumn* nullable_col_ptr, const char* data,
                                 size_t len, bool copy_string, bool need_escape) {
        return write_vec_column(slot_desc, nullable_col_ptr, data, len, copy_string, need_escape,
                                1);
    }

    /// Write consecutive rows of the same data.
    bool write_vec_column(const SlotDescriptor* slot_desc, vectorized::IColumn* nullable_col_ptr,
                          const char* data, size_t len, bool copy_string, bool need_escape,
                          size_t rows);
    void unescape_string_on_spot(const char* src, size_t* len);

    void set_collection_delimiter(char collection_delimiter) {
        _collection_delimiter = collection_delimiter;
    }
    void set_map_kv_delimiter(char mapkv_delimiter) { _map_kv_delimiter = mapkv_delimiter; }

    inline void set_escape_char(const char escape) { this->_escape_char = escape; }

private:
    bool _write_data(const TypeDescriptor& type_desc, vectorized::IColumn* nullable_col_ptr,
                     const char* data, size_t len, bool copy_string, bool need_escape, size_t rows,
                     char array_delimiter);

    char _escape_char;

    //struct,array and map delimiter
    char _collection_delimiter;

    //map key and value delimiter
    char _map_kv_delimiter;
};

} // namespace doris
