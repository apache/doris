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

#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

class Type;
class StructType;

class Schema {
public:
    Schema(int schema_id, std::vector<NestedField> columns);

    Schema(std::vector<NestedField> columns);

    int schema_id() const { return _schema_id; }

    const StructType& root_struct() const { return _root_struct; }

    const std::vector<NestedField>& columns() const { return _root_struct.fields(); }

    Type* find_type(int id) const;

    const NestedField* find_field(int id) const;

private:
    static const char NEWLINE = '\n';
    static const std::string ALL_COLUMNS;
    static const int DEFAULT_SCHEMA_ID;

    int _schema_id;
    StructType _root_struct;
    std::unordered_map<int, const NestedField*> _id_to_field;
};

} // namespace iceberg
} // namespace doris