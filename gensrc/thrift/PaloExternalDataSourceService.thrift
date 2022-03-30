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

namespace java org.apache.doris.thrift
namespace cpp doris

include "Exprs.thrift"
include "Opcodes.thrift"
include "Status.thrift"
include "Types.thrift"

// A result set column descriptor. 
// this definition id different from column desc in palo, the column desc in palo only support scalar type, does not support map, array
// so that should convert palo column desc into ExtColumnDesc
struct TExtColumnDesc {
  // The column name as given in the Create .. statement. Always set.
  1: optional string name
  // The column type. Always set.
  2: optional Types.TTypeDesc type
}

// Metadata used to describe the schema (column names, types, comments)
// of result sets.
struct TExtTableSchema {
  // List of columns. Always set.
  1: optional list<TExtColumnDesc> cols
}

struct TExtLiteral {
  1: required Exprs.TExprNodeType node_type
  2: optional Exprs.TBoolLiteral bool_literal
  3: optional Exprs.TDateLiteral date_literal
  4: optional Exprs.TFloatLiteral float_literal
  5: optional Exprs.TIntLiteral int_literal
  6: optional Exprs.TStringLiteral string_literal
  7: optional Exprs.TDecimalLiteral decimal_literal
  8: optional Exprs.TLargeIntLiteral large_int_literal
}

// Binary predicates that can be pushed to the external data source and
// are of the form <col> <op> <val>. Sources can choose to accept or reject
// predicates via the return value of prepare(), see TPrepareResult.
// The column and the value are guaranteed to be type compatible in Impala,
// but they are not necessarily the same type, so the data source
// implementation may need to do an implicit cast.
// > < = != >= <=
struct TExtBinaryPredicate {
  // Column on which the predicate is applied. Always set.
  1: optional TExtColumnDesc col
  // Comparison operator. Always set.
  2: optional Opcodes.TExprOpcode op
  // Value on the right side of the binary predicate. Always set.
  3: optional TExtLiteral value
}

struct TExtInPredicate {
  1: optional bool is_not_in
  // Column on which the predicate is applied. Always set.
  2: optional TExtColumnDesc col
  // Value on the right side of the binary predicate. Always set.
  3: optional list<TExtLiteral> values
}

struct TExtLikePredicate {
  1: optional TExtColumnDesc col
  2: optional TExtLiteral value
}

struct TExtIsNullPredicate {
  1: optional bool is_not_null
  2: optional TExtColumnDesc col
}

struct TExtFunction { 
  1: optional string func_name
  // input parameter column descs
  2: optional list<TExtColumnDesc> cols
  // input parameter column literals
  3: optional list<TExtLiteral> values
}

// a union of all predicates
struct TExtPredicate {
  1: required Exprs.TExprNodeType node_type
  2: optional TExtBinaryPredicate binary_predicate
  3: optional TExtInPredicate in_predicate
  4: optional TExtLikePredicate like_predicate
  5: optional TExtIsNullPredicate is_null_predicate
  6: optional TExtFunction ext_function
}

// A union over all possible return types for a column of data
// Currently only used by ExternalDataSource types
// 
struct TExtColumnData {
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
struct TExtRowBatch {
  // Each TColumnData contains the data for an entire column. Always set.
  1: optional list<TExtColumnData> cols

  // The number of rows returned. For count(*) queries, there may not be
  // any materialized columns so cols will be an empty list and this value
  // will indicate how many rows are returned. When there are materialized
  // columns, this number should be the same as the size of each
  // TColumnData.is_null list.
  2: optional i64 num_rows
}

// Parameters to prepare().
struct TExtPrepareParams {
  // The name of the table. Always set.
  1: optional string table_name

  // A string specified for the table that is passed to the external data source.
  // Always set, may be an empty string.
  2: optional string init_string

  // A list of conjunctive (AND) clauses, each of which contains a list of
  // disjunctive (OR) binary predicates. Always set, may be an empty list.
  3: optional list<list<TExtPredicate>> predicates
}

// Returned by prepare().
struct TExtPrepareResult {
  1: required Status.TStatus status

  // Estimate of the total number of rows returned when applying the predicates indicated
  // by accepted_conjuncts. Not set if the data source does not support providing
  // this statistic.
  2: optional i64 num_rows_estimate

  // Accepted conjuncts. References the 'predicates' parameter in the prepare()
  // call. It contains the 0-based indices of the top-level list elements (the
  // AND elements) that the library will be able to apply during the scan. Those
  // elements that aren’t referenced in accepted_conjuncts will be evaluated by
  // Impala itself.
  3: optional list<i32> accepted_conjuncts
}

// Parameters to open().
struct TExtOpenParams {
  // A unique identifier for the query. Always set.
  1: optional Types.TUniqueId query_id

  // The name of the table. Always set.
  2: optional string table_name

  // A string specified for the table that is passed to the external data source.
  // Always set, may be an empty string.
  3: optional map<string,string> properties    

  // The authenticated user name. Always set.
  4: optional string authenticated_user_name

  // The schema of the rows that the scan needs to return. Always set.
  5: optional TExtTableSchema row_schema

  // The expected size of the row batches it returns in the subsequent getNext() calls.
  // Always set.
  6: optional i32 batch_size

  7: optional list<list<TExtPredicate>> predicates

  // The query limit, if specified.
  8: optional i64 limit
}

// Returned by open().
struct TExtOpenResult {
  1: required Status.TStatus status

  // An opaque handle used in subsequent getNext()/close() calls. Required.
  2: optional string scan_handle
  3: optional list<i32> accepted_conjuncts
}

// Parameters to getNext()
struct TExtGetNextParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string scan_handle    // es search context id
  2: optional i64 offset            // es should check the offset to prevent duplicate rpc calls
}

// Returned by getNext().
struct TExtGetNextResult {
  1: required Status.TStatus status

  // If true, reached the end of the result stream; subsequent calls to
  // getNext() won’t return any more results. Required.
  2: optional bool eos

  // A batch of rows to return, if any exist. The number of rows in the batch
  // should be less than or equal to the batch_size specified in TOpenParams.
  3: optional TExtRowBatch rows
}

// Parameters to close()
struct TExtCloseParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string scan_handle
}

// Returned by close().
struct TExtCloseResult {
  1: required Status.TStatus status
}

// This data source can be considered as the entry of palo's unified external data source
service TExtDataSourceService {
    // 1. palo be call this api to send index, type, shard id to es
    // 2. es will open a search context and prepare data, will return a context id
    TExtOpenResult open(1: TExtOpenParams params);
    // 1. palo be will send a search context id to es 
    // 2. es will find the search context and find a batch rows and send to palo
    // 3. palo will run the remaining predicates when receving data
    // 4. es should check the offset when receive the request
    TExtGetNextResult getNext(1: TExtGetNextParams params);
    // 1. es will release the context when receiving the data
    TExtCloseResult close(1: TExtCloseParams params);
}
