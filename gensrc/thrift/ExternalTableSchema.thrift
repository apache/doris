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

struct TFieldPtr {
    1: optional TField& field_ptr; // `&` is used to generate std::shared_ptr<TField> on cpp.
}

struct TArrayField {
    1: optional TFieldPtr item_field  // Element field of the array
}

struct TMapField {
    1: optional TFieldPtr key_field, // Key field of the map
    2: optional TFieldPtr value_field // Value field of the map
}

struct TStructField {
    1: optional list<TFieldPtr> fields // List of sub-fields for the struct
}

union TNestedField {
    1: TArrayField array_field,
    2: TStructField struct_field,
    3: TMapField map_field
}

struct TField {
    1: optional bool is_optional,
    2: optional i32 id,               // Field unique id
    3: optional string name,          // Field name
    4: optional Types.TColumnType type,  // Corresponding Doris column type
    5: optional TNestedField nestedField  // Nested field definition (for array, struct, or map types)     
}


struct TSchema {
    1: optional i64 schema_id, // Each time a iceberg/hudi/paimon table schema changes, a new schema id is generated.
    
    // Used to represent all columns in the current table, treating all columns in the table as a struct.
    // The reason for not using `list<TField>` is to reduce logical duplication in the code.
    // For example:
    //         desc table: a int ,
    //                     b string
    //         struct<a:int, b string>
    2: optional TStructField root_field 
}
