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

#ifndef DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H
#define DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H

#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "olap/types.h"

namespace doris {

class TabletColumnSchema {
public:
    TabletColumnSchema();
    OLAPStatus init_from_pb(const ColumnPB& column);

    inline int32_t column_id() const { return _column_id; }
    inline bool is_key() const { return _is_key; }
    inline bool is_nullable() const { return _is_nullable; }
    inline bool is_bf_column() const { return _is_bf_column; }
private:
    int32_t _column_id;
    FieldType _type;
    TypeInfo* _type_info;
    bool _is_key;
    bool _is_nullable;
    bool _is_bf_column;
};

class TabletSchema {
public:
    TabletSchema();
    OLAPStatus init_from_pb(const TabletSchemaPB& schema);
    OLAPStatus to_schema_pb(TabletSchemaPB* tablet_meta_pb);
    size_t get_row_size() const;
    size_t get_field_index(const std::string& field_name) const;
private:
    std::vector<TabletColumnSchema> _cols;
    size_t _num_columns;
    size_t _num_key_columns;
    size_t _num_null_columns;
    size_t _num_short_key_columns;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_H
