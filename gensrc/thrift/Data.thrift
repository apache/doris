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

namespace cpp doris
namespace java org.apache.doris.thrift

include "Types.thrift"

// this is a union over all possible return types
struct TCell {
  // TODO: use <type>_val instead of camelcase
  1: optional bool boolVal
  2: optional i32 intVal
  3: optional i64 longVal
  4: optional double doubleVal
  5: optional string stringVal
  6: optional bool isNull
  // add type: date datetime
}

struct TResultRow {
  1: list<TCell> colVals
}

struct TRow {
  1: optional list<TCell> column_value
}

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TResultBatch {
  // mysql result row
  1: required list<binary> rows

  // Indicates whether tuple_data is snappy-compressed
  2: required bool is_compressed

  // packet seq used to check if there has packet lost
  3: required i64 packet_seq

  4: optional map<string,string> attached_infos
}
