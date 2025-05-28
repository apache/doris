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

namespace cpp doris.schema.external
namespace java org.apache.doris.thrift.schema.external

include "Types.thrift"

struct TArrayField {
    1: optional TField& item_field  // 列表的元素字段
}

struct TMapField {
    1: optional TField& key_field,
    2: optional TField& value_field
}

struct TStructField {
    1: optional list<TField> fields  // 结构体的子字段列表
}


union TNestedField {
    1: TArrayField array_field,
    2: TStructField struct_field,
    3: TMapField map_field
}

struct TField {
    1: optional bool is_optional,
    2: optional i32 id,
    3: optional string name,
    4: optional Types.TColumnType type,  // 映射后的 doris type
    5: optional TNestedField nestedField  // 用于扩展 ListField 或 StructField 或 MapField
}


struct TSchema {
    1: optional i64 schema_id,
    2: optional TStructField root_field
}
