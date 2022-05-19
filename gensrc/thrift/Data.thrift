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

// A result set column descriptor. 
// this definition id different from column desc in palo, the column desc in palo only support scalar type, does not support map, array
// so that should convert palo column desc into ExtColumnDesc
struct TThriftIPCColumnDesc {
  // The column name as given in the Create .. statement. Always set.
  1: optional string name
  // The column type. Always set.
  2: optional Types.TPrimitiveType type
}

// A union over all possible return types for a column of data
// Currently only used by ExternalDataSource types
// 
struct TThriftIPCColumn {
  // One element in the list for every row in the column indicating if there is
  // a value in the vals list or a null.
  1: required list<bool> is_null;

  // Only one is set, only non-null values are set. this indicates one column data for a row batch
  2: optional list<bool> bool_vals;
  3: optional binary byte_vals;
  4: optional list<i16> short_vals;
  5: optional list<i32> int_vals;
  6: optional list<i64> long_vals;
  7: optional list<double> double_vals;
  8: optional list<string> string_vals;
  9: optional list<binary> binary_vals;
}

// Serialized batch of rows returned by getNext().
// one row batch contains mult rows, and the result is arranged in column style
struct TThriftIPCRowBatch {
  1: optional list<TThriftIPCColumnDesc> schema 
  // Each TColumnData contains the data for an entire column. Always set.
  2: optional list<TThriftIPCColumn> cols
  // The number of rows returned. For count(*) queries, there may not be
  // any materialized columns so cols will be an empty list and this value
  // will indicate how many rows are returned. When there are materialized
  // columns, this number should be the same as the size of each
  // TColumnData.is_null list.
  3: optional i64 num_rows
}

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TResultBatch {
  // mysql result row
  1: required list<binary> rows

  // Indicates whether tuple_data is snappy-compressed
  2: required bool is_compressed

  // packet seq used to check if there has packet lost
  3: required i64 packet_seq

  // Use thrift as seriazlize data format
  4: optional TThriftIPCRowBatch thrift_row_batch
}


