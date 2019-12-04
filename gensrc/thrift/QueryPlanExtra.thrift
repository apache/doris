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

include "Types.thrift"
include "Status.thrift"
include "Planner.thrift"
include "Descriptors.thrift"

struct TTabletVersionInfo {
  1: required i64 tablet_id
  2: required i64 version
  3: required i64 version_hash
  // i32 for historical reason
  4: required i32 schema_hash
}

struct TQueryPlanInfo {
  1: required Planner.TPlanFragment plan_fragment
  // tablet_id -> TTabletVersionInfo
  2: required map<i64, TTabletVersionInfo> tablet_info
  3: required Descriptors.TDescriptorTable desc_tbl
  // all tablet scan should share one query_id
  4: required Types.TUniqueId query_id
}
