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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_MAPPING_H
#define DORIS_BE_SRC_OLAP_COLUMN_MAPPING_H

#include "olap/wrapper_field.h"

namespace doris {

struct ColumnMapping {
    ColumnMapping() : ref_column(-1), default_value(nullptr) {}
    virtual ~ColumnMapping() {}

    // <0: use default value
    // >=0: use origin column
    int32_t ref_column;
    // normally for default value. stores values for filters
    WrapperField* default_value;
};

typedef std::vector<ColumnMapping> SchemaMapping;

}  // namespace doris
#endif // DORIS_BE_SRC_COLUMN_MAPPING_H
